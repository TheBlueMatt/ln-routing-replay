mod internal;
use internal::DevNullLogger;

use lightning::routing::gossip::{NetworkGraph, NodeId, ReadOnlyNetworkGraph};

/// We demonstrate breaking out the tracking into several categories, allowing us to track the
/// accuracy of two different models across different types of results.
/// Ultimately we end up with `CATEGORIES` outputs, with averages across the types computed in
/// [`results_complete`].
const CATEGORIES: usize = 1 << 3;
/// Entries with this flag are those for hops that succeeded.
const SUCCESS: usize = 1;
/// Entries with this flag fell back to probability estimation without any historical probing
/// results for this channel, using only the channel's capacity to estimate probability.
const NO_DATA: usize = 2;
/// Entries with this flag are for the live bounds model. Entries without this flag are for the LDK
/// historical model.
const LIVE: usize = 4;

/// The simulation state.
///
/// You're free to put whatever you want here.
pub struct State<'a> {
	/// As a demonstration, the default run calculates the probability the LDK historical model
	/// assigns to results.
	scorer: lightning::routing::scoring::ProbabilisticScorer<&'a NetworkGraph<DevNullLogger>, DevNullLogger>,
	/// We demonstrate calculating log-loss of the both the LDK historical model and the more naive
	/// live bounds model.
	///
	/// Each entry is defined as the total log-loss for the categories out outputs as defined by
	/// the above flags.
	log_loss_sum: [f64; CATEGORIES],
	result_count: [u64; CATEGORIES],
}

/// Creates a new [`State`] before any probe results are processed.
pub fn do_setup<'a>(graph: &'a NetworkGraph<DevNullLogger>) -> State {
	State {
		scorer: lightning::routing::scoring::ProbabilisticScorer::new(Default::default(), graph, internal::DevNullLogger),
		log_loss_sum: [0.0; CATEGORIES],
		result_count: [0; CATEGORIES],
	}
}

/// Processes one probe result.
///
/// The network graph as it existed at `result.timestamp` is provided, as well as a reference to
/// the current state.
pub fn process_probe_result(network_graph: ReadOnlyNetworkGraph, result: ProbeResult, state: &mut State) {
	// Note that the dataset will be regularly updated. If you wish your results to be
	// reproducible, you should add an early return here at some cutoff timestamp.

	// Update the model's time
	use lightning::routing::scoring::ScoreUpdate;
	let cur_time = std::time::Duration::from_secs(result.timestamp);
	state.scorer.time_passed(cur_time);

	// For each hop in the path, we add new entries to the `log_loss_sum` and
	// `result_count` state variables, updating entries with the given flags.
	let mut update_data_with_result = |mut flags, mut probability: f64, success| {
		if success {
			flags |= SUCCESS;
		} else {
			flags &= !SUCCESS;
		}
		if !success { probability = 1.0 - probability; }
		if probability < 0.01 {
			// While the model really needs to be tuned to avoid being so incredibly
			// overconfident, in the mean time we cheat a bit to avoid infinite results.
			probability = 0.01;
		}
		state.log_loss_sum[flags] -= probability.log2();
		state.result_count[flags] += 1;
	};

	// At each hop, we add two new entries - one for the LDK historical model and one for the naive
	// live bounds model.
	let mut evaluate_hop = |hop: &DirectedChannel, success| {
		// While the historical model should always have results for us, its possible that the node
		// doing the probing had a different state than the node that generates the gossip
		// snapshots. Thus, if we fail, we simply ignore ths hop.
		let hist_model_probability =
			state.scorer.historical_estimated_payment_success_probability(hop.short_channel_id, &hop.dst_node_id, hop.amount_msat, &Default::default(), true)?;
		let have_hist_results =
			state.scorer.historical_estimated_payment_success_probability(hop.short_channel_id, &hop.dst_node_id, hop.amount_msat, &Default::default(), false)
			.is_some();
		let flags = if have_hist_results { 0 } else { NO_DATA };
		update_data_with_result(flags, hist_model_probability, success);

		let live_model_probability =
			state.scorer.live_estimated_payment_success_probability(hop.short_channel_id, &hop.dst_node_id, hop.amount_msat, &Default::default())
			.expect("We should have some estimated probability, even without past data");
		let have_live_data = state.scorer.estimated_channel_liquidity_range(hop.short_channel_id, &hop.dst_node_id).is_some();
		let flags = LIVE | if have_live_data { 0 } else { NO_DATA };
		update_data_with_result(flags, live_model_probability, success);
		Some(()) // We don't use the return value, but want to use `?` above.
	};

	// Evaluate the model by passing each hop which succeeded as well as the final failing hop to
	// `evaluate_hop`.
	for hop in result.channels_with_sufficient_liquidity.iter() {
		// You can get additional information about the channel from the network_graph:
		let _chan = network_graph.channels().get(&hop.short_channel_id).unwrap();
		evaluate_hop(hop, true);
	}
	if let Some(hop) = &result.channel_that_rejected_payment {
		// You can get additional information about the channel from the network_graph:
		let _chan = network_graph.channels().get(&hop.short_channel_id).unwrap();
		evaluate_hop(hop, false);
	}

	// Update the model with the information we learned
	let mut path = lightning::routing::router::Path {
		hops: Vec::new(),
		blinded_tail: None,
	};
	let mut hops = result.channels_with_sufficient_liquidity.iter().chain(result.channel_that_rejected_payment.iter()).peekable();
	while hops.peek().is_some() {
		// Sadly, we have to munge the `DirectedChannel` into an LDK `RouteHop`.
		let hop = hops.next().unwrap();
		path.hops.push(lightning::routing::router::RouteHop {
			pubkey: hop.dst_node_id.try_into().unwrap(),
			node_features: lightning::types::features::NodeFeatures::empty(),
			short_channel_id: hop.short_channel_id,
			channel_features: lightning::types::features::ChannelFeatures::empty(),
			fee_msat: hop.amount_msat - hops.peek().map(|hop| hop.amount_msat).unwrap_or(0),
			cltv_expiry_delta: 42,
			maybe_announced_channel: true,
		});
	}
	if let Some(hop) = result.channel_that_rejected_payment {
		state.scorer.probe_failed(&path, hop.short_channel_id, cur_time);
	} else {
		state.scorer.probe_successful(&path, cur_time);
	}
}

/// This is run after all probe results have been processed, and should be used for printing
/// results or any required teardown.
pub fn results_complete(state: State) {
	// We break out log-loss for failure and success hops and print averages between the two
	// (rather than in aggregate) as there are substantially more succeeding hops than there are
	// failing hops.
	for category in 0..CATEGORIES / 4 {
		let flags = category * 4;
		let mut category_name = String::new();
		if (flags & LIVE) != 0 {
			category_name += "Live Bounds Model";
		} else {
			category_name += "Historical Model ";
		}
		for no_data in 0..2 {
			let flags = flags + no_data * NO_DATA;
			let fail_res = state.log_loss_sum[flags] / state.result_count[flags] as f64;
			let suc_res = state.log_loss_sum[flags|1] / state.result_count[flags|1] as f64;
			let mut category_name = category_name.clone();
			if (flags & NO_DATA) != 0 {
				category_name += " (w/ insufficient data)";
			} else {
				category_name += " (w/ some channel hist)";
			}
			println!("Avg {} success log-loss: {}", category_name, suc_res);
			println!("Avg {} failure log-loss: {}", category_name, fail_res);
			println!("Avg {} average log-loss: {}", category_name, (suc_res + fail_res) / 2.0);
		}
		let fail_res = (state.log_loss_sum[flags] + state.log_loss_sum[flags + NO_DATA])
			/ (state.result_count[flags] + state.result_count[flags + NO_DATA]) as f64;
		let suc_res = (state.log_loss_sum[flags + 1] + state.log_loss_sum[flags + NO_DATA + 1])
			/ (state.result_count[flags + 1] + state.result_count[flags + NO_DATA + 1]) as f64;
		println!("Avg {} success log-loss: {}", category_name, suc_res);
		println!("Avg {} failure log-loss: {}", category_name, fail_res);
		println!("Avg {} average log-loss: {}", category_name, (suc_res + fail_res) / 2.0);
		println!();
	}
}

/// A hop in a route, consisting of a channel and the source public key, as well as the amount
/// which we were (or were not) able to send over the channel.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct DirectedChannel {
	/// The source node of a channel
	pub src_node_id: NodeId,
	/// The target node of this channel hop
	pub dst_node_id: NodeId,
	/// The SCID of the channel
	pub short_channel_id: u64,
	/// The amount which may (or may not) have been sendable over this channel
	pub amount_msat: u64,
}

/// The result of a probe through the lightning network.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ProbeResult {
	/// The time at which the probe was completed (i.e. we got the result), as a UNIX timestamp.
	pub timestamp: u64,
	/// Channels over which the probe *succeeded*.
	///
	/// Note that these skip the first hop of the payment, as the first hop is not something which
	/// needs to be predicted (we know our own local channel balances).
	pub channels_with_sufficient_liquidity: Vec<DirectedChannel>,
	/// The channel at which the probe failed, if any
	pub channel_that_rejected_payment: Option<DirectedChannel>,
}

fn main() {
	internal::main();
}
