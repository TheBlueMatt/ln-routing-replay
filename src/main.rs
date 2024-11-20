mod internal;

use lightning::routing::gossip::{NodeId, ReadOnlyNetworkGraph};

/// The simulation state.
///
/// You're free to put whatever you want here.
pub struct State {
}

/// Creates a new [`State`] before any probe results are processed.
pub fn do_setup() -> State {
	State {}
}

/// Processes one probe result.
///
/// The network graph as it existed at `result.timestamp` is provided, as well as a reference to
/// the current state.
pub fn process_probe_result(network_graph: ReadOnlyNetworkGraph, result: ProbeResult, state: &mut State) {

}

/// This is run after all probe results have been processed, and should be used for printing
/// results or any required teardown.
pub fn results_complete(state: State) {

}

/// A hop in a route, consisting of a channel and the source public key, as well as the amount
/// which we were (or were not) able to send over the channel.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct DirectedChannel {
	/// The source node of a channel
	pub src_node_id: NodeId,
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
