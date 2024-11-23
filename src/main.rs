mod internal;
use internal::DevNullLogger;

use lightning::routing::gossip::{NetworkGraph, NodeId, ReadOnlyNetworkGraph};

/// The simulation state.
///
/// You're free to put whatever you want here.
pub struct State<'a> {
	/// As a demonstration, the default run calculates the probability the LDK historical model
	/// assigns to results.
	scorer: lightning::routing::scoring::ProbabilisticScorer<&'a NetworkGraph<DevNullLogger>, DevNullLogger>,
	// We demonstrate calculating log-loss of the LDK historical model
	success_loss_sum: f64,
	success_result_count: u64,
	failure_loss_sum: f64,
	failure_result_count: u64,
	no_data_success_loss_sum: f64,
	no_data_success_result_count: u64,
	no_data_failure_loss_sum: f64,
	no_data_failure_result_count: u64,
}

/// Creates a new [`State`] before any probe results are processed.
pub fn do_setup<'a>(graph: &'a NetworkGraph<DevNullLogger>) -> State {
	State {
		scorer: lightning::routing::scoring::ProbabilisticScorer::new(Default::default(), graph, internal::DevNullLogger),
		success_loss_sum: 0.0,
		success_result_count: 0,
		failure_loss_sum: 0.0,
		failure_result_count: 0,
		no_data_success_loss_sum: 0.0,
		no_data_success_result_count: 0,
		no_data_failure_loss_sum: 0.0,
		no_data_failure_result_count: 0,
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

	// Evaluate the model
	for hop in result.channels_with_sufficient_liquidity.iter() {
		// You can get additional information about the channel from the network_graph:
		let _chan = network_graph.channels().get(&hop.short_channel_id).unwrap();
		let mut no_data = false;
		let mut model_probability =
			state.scorer.historical_estimated_payment_success_probability(hop.short_channel_id, &hop.dst_node_id, hop.amount_msat, &Default::default())
			.unwrap_or_else(|| {
				no_data = true;
				// If LDK doesn't have sufficient historical state it will fall back to (roughly) the live model.
				state.scorer
					.live_estimated_payment_success_probability(hop.short_channel_id, &hop.dst_node_id, hop.amount_msat, &Default::default())
					.expect("We should have some estimated probability, even without history data")
			});
		if model_probability < 0.01 { model_probability = 0.01; }
		state.success_loss_sum -= model_probability.log2();
		state.success_result_count += 1;
		if no_data {
			state.no_data_success_loss_sum -= model_probability.log2();
			state.no_data_success_result_count += 1;
		}
	}
	if let Some(hop) = &result.channel_that_rejected_payment {
		// You can get additional information about the channel from the network_graph:
		let _chan = network_graph.channels().get(&hop.short_channel_id).unwrap();
		let mut no_data = false;
		let mut model_probability =
			state.scorer.historical_estimated_payment_success_probability(hop.short_channel_id, &hop.dst_node_id, hop.amount_msat, &Default::default())
			.unwrap_or_else(|| {
				no_data = true;
				// If LDK doesn't have sufficient historical state it will fall back to (roughly) the live model.
				state.scorer
					.live_estimated_payment_success_probability(hop.short_channel_id, &hop.dst_node_id, hop.amount_msat, &Default::default())
					.expect("We should have some estimated probability, even without history data")
			});
		if model_probability > 0.99 { model_probability = 0.99; }
		state.failure_loss_sum -= (1.0 - model_probability).log2();
		state.failure_result_count += 1;
		if no_data {
			state.no_data_failure_loss_sum -= (1.0 - model_probability).log2();
			state.no_data_failure_result_count += 1;
		}
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
		state.scorer.payment_path_failed(&path, hop.short_channel_id, cur_time);
	} else {
		state.scorer.payment_path_successful(&path, cur_time);
	}
}

/// This is run after all probe results have been processed, and should be used for printing
/// results or any required teardown.
pub fn results_complete(state: State) {
	// We break out log-loss for failure and success hops and print averages between the two
	// (rather than in aggregate) as there are substantially more succeeding hops than there are
	// failing hops.
	let no_data_suc = state.no_data_success_loss_sum / (state.no_data_success_result_count as f64);
	let no_data_fail = state.no_data_failure_loss_sum / (state.no_data_failure_result_count as f64);
	println!("Avg no-data success log-loss            {}", no_data_suc);
	println!("Avg no-data failure log-loss            {}", no_data_fail);
	println!("Avg no-data success+failure log-loss    {}", (no_data_suc + no_data_fail) / 2.0);
	println!();
	let avg_hist_suc = (state.success_loss_sum - state.no_data_success_loss_sum) / ((state.success_result_count - state.no_data_success_result_count) as f64);
	let avg_hist_fail = (state.failure_loss_sum - state.no_data_failure_loss_sum) / ((state.failure_result_count - state.no_data_failure_result_count) as f64);
	println!("Avg historical data success log-loss    {}", avg_hist_suc);
	println!("Avg historical data failure log-loss    {}", avg_hist_fail);
	println!("Avg hist data suc+fail average log-loss {}", (avg_hist_suc + avg_hist_fail) / 2.0);
	println!();
	let avg_suc = state.success_loss_sum / (state.success_result_count as f64);
	let avg_fail = state.failure_loss_sum / (state.failure_result_count as f64);
	println!("Avg success log-loss                    {}", avg_suc);
	println!("Avg failure log-loss                    {}", avg_fail);
	println!("Avg success+failure average log-loss    {}", (avg_suc + avg_fail) / 2.0);
	println!();
	let loss_sum = state.success_loss_sum + state.failure_loss_sum;
	let result_count = state.success_result_count + state.failure_result_count;
	println!("Avg log-loss {}", loss_sum / (result_count as f64));
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
