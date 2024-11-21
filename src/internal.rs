use crate::{do_setup, process_probe_result, results_complete, DirectedChannel, ProbeResult};

use bitcoin::constants::ChainHash;
use bitcoin::network::Network;
use bitcoin::hex::FromHex;
use bitcoin::{Amount, TxOut};

use lightning::ln::chan_utils::make_funding_redeemscript;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::routing::gossip::{NetworkGraph, NodeId, ReadOnlyNetworkGraph};
use lightning::routing::utxo::{UtxoLookup, UtxoResult};
use lightning::util::logger::{Logger, Record};
use lightning::util::ser::Readable;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::FromStr;
use std::sync::Mutex;

struct DevNullLogger;
impl Logger for DevNullLogger {
	fn log(&self, record: Record) {
		#[cfg(debug_assertions)]
		eprintln!("{}", record.args);
	}
}

fn open_file(f: &'static str) -> impl Iterator<Item = String> {
	match File::open(f) {
		Ok(file) => BufReader::new(file).lines().into_iter().map(move |res| match res {
			Ok(line) => line,
			Err(e) => {
				eprintln!("Failed to read line from file {}: {:?}", f, e);
				std::process::exit(1);
			},
		}),
		Err(e) => {
			eprintln!("Failed to open {}, please fetch from https://bitcoin.ninja/ln-routing-replay/{}: {:?}", f, f, e);
			std::process::exit(1);
		}
	}
}

#[derive(Debug)]
struct ParsedChannel {
	scid: u64,
	amount: u64,
}

#[derive(Debug)]
struct ParsedProbeResult {
	timestamp: u64,
	starting_node: NodeId,
	passed_chans: Vec<ParsedChannel>,
	failed_chan: Option<ParsedChannel>,
}

impl ParsedProbeResult {
	fn into_pub(self, graph: ReadOnlyNetworkGraph) -> ProbeResult {
		let mut channels_with_sufficient_liquidity = Vec::new();
		let mut src_node_id = self.starting_node;
		for hop in self.passed_chans {
			channels_with_sufficient_liquidity.push(DirectedChannel {
				src_node_id,
				short_channel_id: hop.scid,
				amount_msat: hop.amount,
			});
			let chan = graph.channels().get(&hop.scid)
				.expect("Missing channel in probe results");
			src_node_id =
				if chan.node_one == src_node_id { chan.node_two } else { chan.node_one };
		}
		let channel_that_rejected_payment = self.failed_chan.map(|hop|
			DirectedChannel {
				src_node_id,
				short_channel_id: hop.scid,
				amount_msat: hop.amount,
			}
		);
		ProbeResult {
			timestamp: self.timestamp,
			channels_with_sufficient_liquidity,
			channel_that_rejected_payment,
		}
	}
}

fn parse_probe(line: String) -> Option<ParsedProbeResult> {
	macro_rules! dbg_unw { ($val: expr) => { { let v = $val; debug_assert!(v.is_some()); v? } } }
	let timestamp_string = dbg_unw!(line.split_once(' ')).0;
	let timestamp = dbg_unw!(timestamp_string.parse::<u64>().ok());
	let useful_out = dbg_unw!(line.split_once(" - start at ")).1;
	let (src_node, path_string) = dbg_unw!(useful_out.split_once(" "));
	let mut passed_chans = Vec::new();
	let mut failed_chan = None;
	for hop in path_string.split(',') {
		let (_ldk_prob, fields) = dbg_unw!(hop.split_once('('));
		let mut fields = fields.split(' ');
		let amount_string = dbg_unw!(fields.next());
		let amount = dbg_unw!(amount_string.parse::<u64>().ok());
		if dbg_unw!(fields.next()) != "msat" { debug_assert!(false); return None; }
		if dbg_unw!(fields.next()) != "on" { debug_assert!(false); return None; }
		let mut scid_string = dbg_unw!(fields.next());
		scid_string = scid_string.strip_suffix(')').unwrap_or(scid_string);
		let scid = dbg_unw!(scid_string.parse::<u64>().ok());
		let chan = ParsedChannel {
			scid,
			amount,
		};
		if hop.starts_with("inv ") {
			if failed_chan.is_some() { debug_assert!(false); return None; }
			failed_chan = Some(chan);
		} else {
			passed_chans.push(chan);
		}
	}
	Some(ParsedProbeResult {
		timestamp,
		starting_node: dbg_unw!(NodeId::from_str(src_node).ok()),
		passed_chans,
		failed_chan,
	})
}

fn parse_update(line: String) -> Option<(u64, ChannelUpdate)> {
	let (timestamp, update_hex) = line.split_once('|').expect("Invalid ChannelUpdate line");
	let update_bytes = Vec::<u8>::from_hex(update_hex).ok().expect("Invalid ChannelUpdate hex");
	let update = Readable::read(&mut &update_bytes[..]);
	debug_assert!(update.is_ok());
	Some((timestamp.parse().ok()?, update.ok()?))
}

fn parse_announcement(line: String) -> Option<(u64, u64, ChannelAnnouncement)> {
	let mut fields = line.split('|');
	let timestamp = fields.next().expect("Invalid ChannelAnnouncement line");
	let funding_sats = fields.next().expect("Invalid ChannelAnnouncement line");
	let ann_hex = fields.next().expect("Invalid ChannelAnnouncement line");
	let ann_bytes = Vec::<u8>::from_hex(ann_hex).ok().expect("Invalid ChannelAnnouncement hex");
	let announcement = Readable::read(&mut &ann_bytes[..]);
	debug_assert!(announcement.is_ok());
	Some((timestamp.parse().ok()?, funding_sats.parse().ok()?, announcement.ok()?))
}

struct FundingValueProvider {
	channel_values: Mutex<HashMap<u64, TxOut>>,
}

impl UtxoLookup for FundingValueProvider {
	fn get_utxo(&self, _: &ChainHash, scid: u64) -> UtxoResult {
		UtxoResult::Sync(Ok(
			self.channel_values.lock().unwrap().get(&scid).expect("Missing Channel Value").clone()
		))
	}
}

pub fn main() {
	let graph = NetworkGraph::new(Network::Bitcoin, &DevNullLogger);
	let mut updates = open_file("channel_updates.txt").filter_map(parse_update).peekable();
	let mut announcements = open_file("channel_announcements.txt").filter_map(parse_announcement).peekable();
	let mut probe_results = open_file("probes.txt").filter_map(parse_probe).peekable();

	let channel_values = Mutex::new(HashMap::new());
	let utxo_values = FundingValueProvider { channel_values };
	let utxo_lookup = Some(&utxo_values);

	let mut state = do_setup();

	loop {
		let next_update = updates.peek();
		let next_announcement = announcements.peek();
		let next_probe_result = probe_results.peek();

		if next_update.is_none() && next_announcement.is_none() && next_probe_result.is_none() {
			break;
		}

		let next_update_ts = next_update.map(|(t, _)| *t).unwrap_or(u64::MAX);
		let next_announce_ts = next_announcement.map(|(t, _, _)| *t).unwrap_or(u64::MAX);
		let next_probe_ts = next_probe_result.map(|res| res.timestamp).unwrap_or(u64::MAX);
		match (next_update_ts < next_announce_ts, next_announce_ts < next_probe_ts, next_update_ts < next_probe_ts) {
			(true, _, true) => {
				if let Some((_, update)) = updates.next() {
					let res = graph.update_channel_unsigned(&update.contents);
					if let Err(e) = res {
						debug_assert_eq!(e.err, "Update older than last processed update");
					}
				} else { unreachable!() }
			}
			(false, true, _) => {
				if let Some((_, funding_sats, announcement)) = announcements.next() {
					let a_key = announcement.contents.bitcoin_key_1.try_into().unwrap();
					let b_key = announcement.contents.bitcoin_key_2.try_into().unwrap();
					let script_pubkey = make_funding_redeemscript(&a_key, &b_key).to_p2wsh();
					let txout = TxOut { script_pubkey, value: bitcoin::Amount::from_sat(funding_sats) };
					utxo_values.channel_values.lock().unwrap().insert(announcement.contents.short_channel_id, txout);
					graph.update_channel_from_unsigned_announcement(&announcement.contents, &utxo_lookup)
						.expect("announcements should be valid");
				} else { unreachable!() }
			}
			_ => {
				if let Some(res) = probe_results.next() {
					process_probe_result(graph.read_only(), res.into_pub(graph.read_only()), &mut state);
				} else { unreachable!() }
			}
		}
	}

	results_complete(state);
}
