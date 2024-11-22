use crate::{do_setup, process_probe_result, results_complete, DirectedChannel, ProbeResult};

use bitcoin::constants::ChainHash;
use bitcoin::network::Network;
use bitcoin::{Amount, TxOut};

use lightning::ln::chan_utils::make_funding_redeemscript;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::routing::gossip::{NetworkGraph, NodeId, ReadOnlyNetworkGraph};
use lightning::routing::utxo::{UtxoLookup, UtxoResult};
use lightning::util::logger::{Logger, Record};
use lightning::util::ser::Readable;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Mutex;

pub struct DevNullLogger;
impl Logger for DevNullLogger {
	fn log(&self, _: Record) {}
}
/// Dirty hack to make `DevNullLogger` a `Deref`.
impl Deref for DevNullLogger {
	type Target = DevNullLogger;
	fn deref(&self) -> &DevNullLogger { self }
}

fn open_file(f: &'static str) -> BufReader<File> {
	match File::open(f) {
		Ok(file) => BufReader::new(file),
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
	fn try_into_pub(self, graph: ReadOnlyNetworkGraph) -> Option<ProbeResult> {
		let mut channels_with_sufficient_liquidity = Vec::new();
		let mut src_node_id = self.starting_node;
		for hop in self.passed_chans {
			let chan = graph.channels().get(&hop.scid)
				.expect("Missing channel in probe results");
			if chan.one_to_two.is_none() || chan.two_to_one.is_none() {
				// This shouldn't ever happen as LDK shouldn't have actually tried to send over
				// this path, but sometimes it does (presumably due to some delayed graph pruning).
				// Its fairly rare, and results in less data than the model under test might want,
				// so we just skip such paths.
				return None;
			}
			let dst_node_id =
				if chan.node_one == src_node_id { chan.node_two } else { chan.node_one };
			channels_with_sufficient_liquidity.push(DirectedChannel {
				src_node_id,
				dst_node_id,
				short_channel_id: hop.scid,
				amount_msat: hop.amount,
			});
			src_node_id = dst_node_id;
		}
		let channel_that_rejected_payment = self.failed_chan.map(|hop| {
			let chan = graph.channels().get(&hop.scid)
				.expect("Missing channel in probe results");
			let dst_node_id =
				if chan.node_one == src_node_id { chan.node_two } else { chan.node_one };
			DirectedChannel {
				src_node_id,
				dst_node_id,
				short_channel_id: hop.scid,
				amount_msat: hop.amount,
			}
		});
		Some(ProbeResult {
			timestamp: self.timestamp,
			channels_with_sufficient_liquidity,
			channel_that_rejected_payment,
		})
	}
}

fn parse_probe(line: Result<String, std::io::Error>) -> Option<ParsedProbeResult> {
	if line.is_err() {
		eprintln!("Failed to read line from parse results: {:?}", line);
		std::process::exit(1);
	}
	let line = line.unwrap();
	macro_rules! dbg_unw { ($val: expr) => { { let v = $val; debug_assert!(v.is_some()); v? } } }
	if line.contains("unknown path success prob, probably had a duplicate") {
		return None;
	}
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

struct UpdateIter(BufReader<File>);
impl Iterator for UpdateIter {
	type Item = (u64, ChannelUpdate);
	fn next(&mut self) -> Option<(u64, ChannelUpdate)> {
		macro_rules! read { ($bytes: expr) => { {
			if let Err(_) = self.0.read_exact(&mut $bytes[..]) {
				return None;
			}
		} } }
		let mut ts_bytes = [0u8; 8];
		read!(ts_bytes);
		let mut len_bytes = [0u8; 2];
		read!(len_bytes);
		let len = u16::from_le_bytes(len_bytes) as usize;
		let mut update_bytes = vec![0; len];
		read!(update_bytes);
		let update = Readable::read(&mut &update_bytes[..]);
		debug_assert!(update.is_ok());
		Some((u64::from_le_bytes(ts_bytes), update.ok()?))
	}
}

struct AnnouncementIter(BufReader<File>);
impl Iterator for AnnouncementIter {
	type Item = (u64, u64, ChannelAnnouncement);
	fn next(&mut self) -> Option<(u64, u64, ChannelAnnouncement)> {
		macro_rules! read { ($bytes: expr) => { {
			if let Err(_) = self.0.read_exact(&mut $bytes[..]) {
				return None;
			}
		} } }
		let mut ts_bytes = [0u8; 8];
		read!(ts_bytes);
		let mut funding_bytes = [0u8; 8];
		read!(funding_bytes);
		let mut len_bytes = [0u8; 2];
		read!(len_bytes);
		let len = u16::from_le_bytes(len_bytes) as usize;
		let mut announcement_bytes = vec![0; len];
		read!(announcement_bytes);
		let announcement = Readable::read(&mut &announcement_bytes[..]);
		debug_assert!(announcement.is_ok());
		Some((u64::from_le_bytes(ts_bytes), u64::from_le_bytes(funding_bytes), announcement.ok()?))
	}
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
	let graph = NetworkGraph::new(Network::Bitcoin, DevNullLogger);
	let mut updates = UpdateIter(open_file("channel_updates.bin")).peekable();
	let mut announcements = AnnouncementIter(open_file("channel_announcements.bin")).peekable();
	let mut probe_results = open_file("probes.txt").lines().into_iter().filter_map(parse_probe).peekable();

	let channel_values = Mutex::new(HashMap::new());
	let utxo_values = FundingValueProvider { channel_values };
	let utxo_lookup = Some(&utxo_values);

	let mut state = do_setup(&graph);

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
					let txout = TxOut { script_pubkey, value: Amount::from_sat(funding_sats) };
					utxo_values.channel_values.lock().unwrap().insert(announcement.contents.short_channel_id, txout);
					graph.update_channel_from_unsigned_announcement(&announcement.contents, &utxo_lookup)
						.expect("announcements should be valid");
				} else { unreachable!() }
			}
			_ => {
				if let Some(res) = probe_results.next() {
					if let Some(res) = res.try_into_pub(graph.read_only()) {
						process_probe_result(graph.read_only(), res, &mut state);
					}
				} else { unreachable!() }
			}
		}
	}

	results_complete(state);
}
