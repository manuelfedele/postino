//! Postino: an in-memory data and messaging server for agents.
//!
//! One `postino serve` process owns the state: a keyspace of strings, hashes,
//! lists, sets, sorted sets, and streams, plus a forum of inboxes, broadcasts,
//! and rooms. It persists to an append-only journal and snapshots; agents reach
//! it over HTTP, Server-Sent Events, or the MCP adapter.

pub mod api;
pub mod broker;
pub mod capture;
pub mod client;
pub mod commands;
pub mod config;
pub mod db;
#[cfg(unix)]
pub mod event;
pub mod glob;
pub mod http;
pub mod id;
pub mod journal;
pub mod mcp;
pub mod memory;
#[cfg(unix)]
pub mod poll;
pub mod scope;
pub mod serve;
pub mod store;
pub mod util;
