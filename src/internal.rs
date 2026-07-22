use crate::{do_setup, process_probe_result, results_complete, DirectedChannel, ProbeResult};

use bitcoin::constants::ChainHash;
use bitcoin::network::Network;
use bitcoin::{Amount, TxOut};

use lightning::ln::chan_utils::make_funding_redeemscript;
use lightning::ln::msgs::{ChannelAnnouncement, ChannelUpdate};
use lightning::routing::gossip::{NetworkGraph, NodeId, ReadOnlyNetworkGraph};
use lightning::routing::utxo::{UtxoLookup, UtxoResult};
use lightning::util::logger::{Logger, Record};
use lightning::util::ser::LengthReadable;

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
			let chan = graph.channels().get(&hop.scid);
			if chan.is_none() {
				// This shouldn't ever happen as LDK shouldn't have actually tried to send over
				// this path, but sometimes it does as the graph datasource and probing data
				// datasources are different.
				// Its fairly rare, and results in less data than the model under test might want,
				// so we just skip such paths.
				return None;
			}
			let chan = chan.unwrap();
			if chan.one_to_two.is_none() || chan.two_to_one.is_none() {
				// This shouldn't ever happen as LDK shouldn't have actually tried to send over
				// this path, but sometimes it does as the graph datasource and probing data
				// datasources are different.
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
		let chan_hop = self.failed_chan.map(|hop| (graph.channels().get(&hop.scid), hop));
		if let Some((None, _)) = chan_hop {
			// This shouldn't ever happen as LDK shouldn't have actually tried to send over
			// this path, but sometimes it does as the graph datasource and probing data
			// datasources are different.
			// Its fairly rare, and results in less data than the model under test might want,
			// so we just skip such paths.
			return None;
		}
		let channel_that_rejected_payment = chan_hop.map(|(chan, hop)| {
			let chan = chan.unwrap();
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
	macro_rules! dbg_unw { ($val: expr) => { { let v = $val; debug_assert!(v.is_some(), "{}", line); v? } } }
	if line.contains("unknown path success prob, probably had a duplicate") {
		return None;
	}
	if line.contains("path failed at first hop") {
		return None;
	}
	let timestamp_string = dbg_unw!(line.split_once(' ')).0;
	let timestamp = dbg_unw!(timestamp_string.parse::<u64>().ok());
	let useful_out = dbg_unw!(line.split_once(" - start at ")).1;
	let (src_node, path_string) = dbg_unw!(useful_out.split_once(" "));
	let mut passed_chans = Vec::new();
	let mut failed_chan = None;
	for hop in path_string.split(',') {
		let hop = hop.trim_start();
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
		let update = LengthReadable::read_from_fixed_length_buffer(&mut &update_bytes[..]);
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
		let announcement = LengthReadable::read_from_fixed_length_buffer(&mut &announcement_bytes[..]);
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
	let probe_count = open_file("probes.txt").lines().count();
	let mut probe_results = open_file("probes.txt").lines().into_iter().filter_map(parse_probe).enumerate().peekable();

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
		let next_probe_ts = next_probe_result.map(|(_, res)| res.timestamp).unwrap_or(u64::MAX);
		match (next_update_ts < next_announce_ts, next_announce_ts < next_probe_ts, next_update_ts < next_probe_ts) {
			(true, _, true) => {
				if let Some((_, update)) = updates.next() {
					let res = graph.update_channel_unsigned(&update.contents);
					if let Err(e) = res {
						if e.err == "Couldn't find channel for update" {
							// The data bakend had an issue where some channel announcements were missed on Feb 28, 2025
							// The channel announcements which were missed were:
							let missed_announcements = [
								973586760097005568, 973624143520202753, 973625243044675585, 973626342543327233,
								973626342632849408, 973630740605829121, 973634039130554369, 973636238184546304,
								973642835298484224, 973642835301564416, 973646133814362113, 973657128961310720,
								973658228347174913, 973659327877808128, 973659327886131201, 973665925117509632,
								973670323021086720, 973671422523277312, 973671422581735424, 973671422603100161,
								973674721120419841, 973675820620316672, 973680218656604161, 973686815697469440,
								973692313220087808, 973692313263210496, 973692313318260736, 973694512391782400,
								973700009956343808, 973700009958768641, 973700009959751681, 973704408018321409,
								973704408019959809, 973706606883569664, 973706606888026112, 973706606892023808,
								973706606894972928, 973706606895366145, 973706606895562753, 973707706523910145,
								973709905432608768, 973712104476573697, 973712104479326208, 973712104479391744,
								973713203953008641, 973713203953270785, 973716502547464192, 973716502591635457,
								973720900605771777, 973722000028401665, 973725298581504000, 973725298581766144,
								973725298600181760, 973734094786265088, 973734094800879616, 973737393326850049,
								973738492692922368, 973738492739715072, 973739592341127168, 973740691871039489,
								973741791322505217, 973741791409274880, 973745089883406336, 973745089890287617,
								973746189452705793, 973746189475381248, 973747288840863745, 973752786462834688,
								973754985411903489, 973754985411969024, 973756084915208193, 973756084997718017,
								973760483120447489, 973767080030044161, 973767080056782849, 973767080165376001,
								973767080165507073, 973767080166752257, 973767080166817793, 973768179546718208,
								973770378515644416, 973771478060236800, 973773677113638912, 973779174720077824,
								// Additionally, for unknown reasons, two further announcements are
								// missing:
								1035327636750991365, 1040915354625507329,
							];
							debug_assert!(missed_announcements.iter().any(|scid| *scid == update.contents.short_channel_id));
						} else {
							debug_assert_eq!(e.err, "Update older than last processed update");
						}
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
				if let Some((probe_id, res)) = probe_results.next() {
					if let Some(res) = res.try_into_pub(graph.read_only()) {
						process_probe_result(graph.read_only(), res, &mut state);
					}
					if probe_id % (probe_count / 20) == 0 {
						println!("Processed {}/{} probes", probe_id, probe_count);
					}
				} else { unreachable!() }
			}
		}
	}

	results_complete(state);
}
