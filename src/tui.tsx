import React, { useCallback, useEffect, useRef, useState } from "react";
import { Box, Text, render, useApp, useInput, useStdin, useStdout } from "ink";
import {
  acknowledgeMessages,
  appendBroadcast,
  appendMessage,
  connect,
  deregisterAgent,
  getInboxCount,
  getOnlineAgents,
  leaseMessages,
  listBroadcasts,
  listInbox,
  publishEvent,
  registerAgent,
  valkey,
  keys,
  disconnect,
} from "./valkey.js";
import type { Broadcast, Config, Message } from "./types.js";

type Mode =
  | "inbox"
  | "broadcasts"
  | "compose-message"
  | "compose-broadcast"
  | "help";
type Tone = "good" | "warning" | "critical" | "info";

type AgentRow = {
  name: string;
  online: boolean;
  messages: number;
};

type Notice = {
  tone: Tone;
  text: string;
};

const COLORS = {
  ground: "#0B1622",
  panel: "#0F1E2C",
  raised: "#132738",
  rule: "#2C4B63",
  ink: "#DCE8F0",
  muted: "#6F8CA3",
  secondary: "#9DB4C7",
  amber: "#E8A33D",
  teal: "#0BAA92",
  violet: "#9B7BE9",
  good: "#3FB27F",
  warning: "#D6C43C",
  critical: "#E2555B",
};

const PANEL_WIDTH = 112;
const RAIL_WIDTH = 28;
const MAX_VISIBLE_MESSAGES = 8;

function shortTime(timestamp: string): string {
  return new Date(timestamp).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function shortDate(timestamp: string): string {
  const date = new Date(timestamp);
  return `${date.toLocaleDateString([], { month: "short", day: "numeric" })} ${shortTime(timestamp)}`;
}

function toneColor(tone: Tone): string {
  if (tone === "good") return COLORS.good;
  if (tone === "warning") return COLORS.warning;
  if (tone === "critical") return COLORS.critical;
  return COLORS.secondary;
}

function StatusDot({ online }: { online: boolean }) {
  return (
    <Text color={online ? COLORS.good : COLORS.muted}>
      {online ? "●" : "○"}
    </Text>
  );
}

function Header({
  config,
  mode,
  onlineCount,
  lastUpdated,
}: {
  config: Config;
  mode: Mode;
  onlineCount: number;
  lastUpdated: string;
}) {
  return (
    <Box
      flexDirection="column"
      borderStyle="round"
      borderColor={COLORS.rule}
      paddingX={1}
    >
      <Box justifyContent="space-between">
        <Box>
          <Text color={COLORS.amber} bold>
            POSTINO
          </Text>
          <Text color={COLORS.muted}> // CONTROL SURFACE</Text>
        </Box>
        <Text color={COLORS.good}>● LINKED</Text>
      </Box>
      <Box marginTop={1} justifyContent="space-between">
        <Box>
          <Text color={COLORS.secondary}>AGENT </Text>
          <Text color={COLORS.amber} bold>
            {config.agentName}
          </Text>
        </Box>
        <Box>
          <Text color={COLORS.muted}>VIEW </Text>
          <Text color={COLORS.ink} bold>
            {mode === "compose-message"
              ? "MESSAGE"
              : mode === "compose-broadcast"
                ? "BROADCAST"
                : mode.toUpperCase()}
          </Text>
          <Text color={COLORS.muted}> ONLINE </Text>
          <Text color={COLORS.good} bold>
            {onlineCount}
          </Text>
          <Text color={COLORS.muted}> SYNC </Text>
          <Text color={COLORS.secondary}>{lastUpdated || "--:--"}</Text>
        </Box>
      </Box>
    </Box>
  );
}

function Tabs({ mode }: { mode: Mode }) {
  const tab = (name: string, active: boolean, key: string) => (
    <Box key={key} marginRight={2}>
      <Text color={active ? COLORS.amber : COLORS.muted} bold={active}>
        {active ? "[" : " "}
        {name}
        {active ? "]" : " "}
      </Text>
    </Box>
  );

  return (
    <Box marginTop={1}>
      {tab("INBOX", mode === "inbox" || mode === "compose-message", "inbox")}
      {tab(
        "BROADCASTS",
        mode === "broadcasts" || mode === "compose-broadcast",
        "broadcasts",
      )}
      {tab("HELP", mode === "help", "help")}
    </Box>
  );
}

function AgentRail({
  agents,
  selectedIndex,
  onSelect,
}: {
  agents: AgentRow[];
  selectedIndex: number;
  onSelect: (index: number) => void;
}) {
  return (
    <Box
      width={RAIL_WIDTH}
      flexDirection="column"
      borderStyle="round"
      borderColor={COLORS.rule}
      paddingX={1}
    >
      <Box justifyContent="space-between">
        <Text color={COLORS.amber} bold>
          AGENTS
        </Text>
        <Text color={COLORS.muted}>{agents.length}</Text>
      </Box>
      <Text color={COLORS.muted}>──────────────</Text>
      <Box flexDirection="column" marginTop={1}>
        {agents.length === 0 ? (
          <Text color={COLORS.muted}>No agents</Text>
        ) : (
          agents.map((agent, index) => (
            <Box key={agent.name}>
              <Text
                color={index === selectedIndex ? COLORS.amber : COLORS.muted}
              >
                {index === selectedIndex ? "> " : "  "}
              </Text>
              <StatusDot online={agent.online} />
              <Text
                color={index === selectedIndex ? COLORS.ink : COLORS.secondary}
                bold={index === selectedIndex}
                wrap="truncate-end"
              >
                {` ${agent.name}`.slice(0, RAIL_WIDTH - 8)}
              </Text>
              <Box flexGrow={1} />
              <Text
                color={agent.messages > 0 ? COLORS.amber : COLORS.muted}
                bold={agent.messages > 0}
              >
                {agent.messages || "-"}
              </Text>
            </Box>
          ))
        )}
      </Box>
      <Box flexGrow={1} />
      <Text color={COLORS.muted}>j/k select</Text>
      <Text color={COLORS.muted}>r refresh</Text>
    </Box>
  );
}

function MessageCard({
  message,
  leased,
}: {
  message: Message;
  leased: boolean;
}) {
  return (
    <Box
      flexDirection="column"
      borderStyle="single"
      borderColor={leased ? COLORS.amber : COLORS.rule}
      paddingX={1}
      marginBottom={1}
    >
      <Box justifyContent="space-between">
        <Box>
          <Text
            color={message.from === "operator" ? COLORS.teal : COLORS.amber}
            bold
          >
            {message.from}
          </Text>
          <Text color={COLORS.muted}> {shortDate(message.timestamp)}</Text>
        </Box>
        <Text color={leased ? COLORS.amber : COLORS.muted}>
          {leased ? "LEASED" : "QUEUED"}
        </Text>
      </Box>
      <Text color={COLORS.ink} wrap="wrap">
        {message.body}
      </Text>
      <Text color={COLORS.muted}>
        id {message.id.slice(0, 18)}
        {message.id.length > 18 ? "..." : ""}
      </Text>
    </Box>
  );
}

function InboxView({
  agent,
  messages,
  leased,
  busy,
}: {
  agent?: AgentRow;
  messages: Message[];
  leased: Message[];
  busy: boolean;
}) {
  const visible = messages.slice(-MAX_VISIBLE_MESSAGES);
  const leasedIds = new Set(leased.map((message) => message.id));
  return (
    <Box
      flexDirection="column"
      flexGrow={1}
      borderStyle="round"
      borderColor={COLORS.rule}
      paddingX={1}
    >
      <Box justifyContent="space-between">
        <Box>
          <Text color={COLORS.amber} bold>
            DIRECT CHANNEL
          </Text>
          <Text color={COLORS.secondary}> / {agent?.name ?? "NO TARGET"}</Text>
        </Box>
        <Box>
          <Text color={COLORS.muted}>{messages.length} queued </Text>
          {busy ? (
            <Text color={COLORS.warning}>working...</Text>
          ) : (
            <Text color={COLORS.muted}>l lease a ack</Text>
          )}
        </Box>
      </Box>
      <Text color={COLORS.muted}>
        ────────────────────────────────────────────────────────────────────────────
      </Text>
      <Box flexDirection="column" marginTop={1}>
        {visible.length === 0 ? (
          <Box flexDirection="column" marginTop={2}>
            <Text color={COLORS.muted}>Inbox clear.</Text>
            <Text color={COLORS.muted}>
              Press <Text color={COLORS.amber}>l</Text> to lease new work when
              it arrives.
            </Text>
          </Box>
        ) : (
          visible.map((message) => (
            <MessageCard
              key={message.id}
              message={message}
              leased={leasedIds.has(message.id)}
            />
          ))
        )}
      </Box>
      <Box flexGrow={1} />
      <Box borderStyle="single" borderColor={COLORS.rule} paddingX={1}>
        <Text color={COLORS.muted}>[m] </Text>
        <Text color={COLORS.secondary}>send a direct message</Text>
        <Text color={COLORS.muted}> [a] </Text>
        <Text color={leased.length > 0 ? COLORS.amber : COLORS.secondary}>
          ack leased ({leased.length})
        </Text>
        <Text color={COLORS.muted}> [l] </Text>
        <Text color={COLORS.secondary}>lease next</Text>
      </Box>
      <Box marginTop={1}>
        <Text color={COLORS.muted}>
          Tip: unacknowledged work returns after the lease window.
        </Text>
      </Box>
      <Box marginTop={1}>
        <Text color={COLORS.amber} bold>
          {agent ? "TARGET READY" : "SELECT AN AGENT"}
        </Text>
        {agent && (
          <Text color={COLORS.muted}>
            {" "}
            {agent.online ? "online" : "offline but reachable"}
          </Text>
        )}
      </Box>
      <Box marginTop={1}>
        <Text color={COLORS.muted}>Press </Text>
        <Text color={COLORS.amber}>m</Text>
        <Text color={COLORS.muted}> to compose.</Text>
      </Box>
    </Box>
  );
}

function BroadcastView({ broadcasts }: { broadcasts: Broadcast[] }) {
  return (
    <Box
      flexDirection="column"
      flexGrow={1}
      borderStyle="round"
      borderColor={COLORS.rule}
      paddingX={1}
    >
      <Box justifyContent="space-between">
        <Box>
          <Text color={COLORS.amber} bold>
            BROADCAST CHANNEL
          </Text>
          <Text color={COLORS.secondary}> / shared announcements</Text>
        </Box>
        <Text color={COLORS.muted}>{broadcasts.length} retained</Text>
      </Box>
      <Text color={COLORS.muted}>
        ────────────────────────────────────────────────────────────────────────────
      </Text>
      <Box flexDirection="column" marginTop={1}>
        {broadcasts
          .slice(-8)
          .reverse()
          .map((broadcast, index) => (
            <Box
              key={broadcast.id}
              flexDirection="column"
              borderStyle="single"
              borderColor={index === 0 ? COLORS.amber : COLORS.rule}
              paddingX={1}
              marginBottom={1}
            >
              <Box>
                <Text color={index === 0 ? COLORS.amber : COLORS.teal} bold>
                  {broadcast.from}
                </Text>
                <Text color={COLORS.muted}>
                  {" "}
                  {shortDate(broadcast.timestamp)}
                </Text>
              </Box>
              <Text color={COLORS.ink} wrap="wrap">
                {broadcast.body}
              </Text>
            </Box>
          ))}
        {broadcasts.length === 0 && (
          <Text color={COLORS.muted}>
            No broadcasts in the retention window.
          </Text>
        )}
      </Box>
      <Box flexGrow={1} />
      <Box borderStyle="single" borderColor={COLORS.rule} paddingX={1}>
        <Text color={COLORS.muted}>[b] </Text>
        <Text color={COLORS.secondary}>new broadcast</Text>
      </Box>
      <Box marginTop={1}>
        <Text color={COLORS.muted}>
          Every agent tracks its own read cursor.
        </Text>
      </Box>
      <Box marginTop={1}>
        <Text color={COLORS.amber} bold>
          SHARED CHANNEL
        </Text>
        <Text color={COLORS.muted}> stable IDs / replayable events</Text>
      </Box>
      <Box marginTop={1}>
        <Text color={COLORS.muted}>Press </Text>
        <Text color={COLORS.amber}>b</Text>
        <Text color={COLORS.muted}> to compose a broadcast.</Text>
      </Box>
    </Box>
  );
}

function ComposeView({
  mode,
  from,
  target,
  draft,
  busy,
}: {
  mode: "compose-message" | "compose-broadcast";
  from: string;
  target?: string;
  draft: string;
  busy: boolean;
}) {
  const isBroadcast = mode === "compose-broadcast";
  return (
    <Box
      flexDirection="column"
      flexGrow={1}
      borderStyle="round"
      borderColor={COLORS.amber}
      paddingX={2}
    >
      <Text color={COLORS.amber} bold>
        {isBroadcast ? "NEW BROADCAST" : "NEW DIRECT MESSAGE"}
      </Text>
      <Text color={COLORS.muted}>
        ────────────────────────────────────────────────────────────────────────────
      </Text>
      <Box marginTop={2} flexDirection="column">
        <Box>
          <Text color={COLORS.muted}>FROM </Text>
          <Text color={COLORS.teal} bold>
            {from}
          </Text>
        </Box>
        <Box>
          <Text color={COLORS.muted}>
            {isBroadcast ? "TO        " : "TO        "}
          </Text>
          <Text color={COLORS.ink} bold>
            {isBroadcast ? "ALL AGENTS" : (target ?? "SELECT AN AGENT")}
          </Text>
        </Box>
      </Box>
      <Box
        marginTop={2}
        borderStyle="single"
        borderColor={COLORS.rule}
        paddingX={1}
      >
        <Text color={COLORS.amber}>{draft.length === 0 ? "> " : "  "}</Text>
        <Text color={draft.length === 0 ? COLORS.muted : COLORS.ink}>
          {draft || "Type your message..."}
        </Text>
        <Text color={COLORS.amber}>_</Text>
      </Box>
      <Box marginTop={2} flexDirection="column">
        <Text color={COLORS.muted}>ENTER send ESC cancel CTRL-U clear</Text>
        <Text color={COLORS.muted}>
          Messages are delivered through the same Valkey queue as MCP clients.
        </Text>
      </Box>
      {busy && (
        <Box marginTop={2}>
          <Text color={COLORS.warning}>Publishing...</Text>
        </Box>
      )}
      <Box flexGrow={1} />
      <Box>
        <Text color={COLORS.muted}>Characters </Text>
        <Text color={COLORS.secondary}>{draft.length}</Text>
        <Text color={COLORS.muted}> / 32768</Text>
      </Box>
    </Box>
  );
}

function HelpView() {
  const rows = [
    ["j / k, arrows", "move through agents"],
    ["i", "open selected inbox"],
    ["b", "open broadcasts / compose"],
    ["m", "compose direct message"],
    ["l", "lease selected inbox"],
    ["a", "acknowledge leased messages"],
    ["r", "refresh state"],
    ["q, Ctrl-C", "quit TUI"],
  ];
  return (
    <Box
      flexDirection="column"
      flexGrow={1}
      borderStyle="round"
      borderColor={COLORS.rule}
      paddingX={2}
    >
      <Text color={COLORS.amber} bold>
        OPERATOR KEYMAP
      </Text>
      <Text color={COLORS.muted}>
        ────────────────────────────────────────────────────────────────────────────
      </Text>
      <Box flexDirection="column" marginTop={2}>
        {rows.map(([key, description]) => (
          <Box key={key}>
            <Box width={18}>
              <Text color={COLORS.amber} bold>
                {key}
              </Text>
            </Box>
            <Text color={COLORS.secondary}>{description}</Text>
          </Box>
        ))}
      </Box>
      <Box marginTop={2} flexDirection="column">
        <Text color={COLORS.muted}>Leasing is intentionally explicit.</Text>
        <Text color={COLORS.muted}>
          An unacknowledged message returns after the configured lease window.
        </Text>
        <Text color={COLORS.muted}>
          This makes the TUI safe for human-in-the-loop operations.
        </Text>
      </Box>
      <Box flexGrow={1} />
      <Text color={COLORS.teal}>POSTINO / HUMAN CONTROL PLANE</Text>
    </Box>
  );
}

function Footer({ notice }: { notice?: Notice }) {
  return (
    <Box marginTop={1} justifyContent="space-between">
      <Box>
        <Text color={COLORS.amber} bold>
          q
        </Text>
        <Text color={COLORS.muted}> quit </Text>
        <Text color={COLORS.amber} bold>
          r
        </Text>
        <Text color={COLORS.muted}> refresh </Text>
        <Text color={COLORS.amber} bold>
          ?
        </Text>
        <Text color={COLORS.muted}> help</Text>
      </Box>
      <Text color={notice ? toneColor(notice.tone) : COLORS.muted}>
        {notice?.text ?? "ready"}
      </Text>
    </Box>
  );
}

function TuiApp({
  config,
  instanceId,
  onExit,
}: {
  config: Config;
  instanceId: string;
  onExit: () => void;
}) {
  const { exit } = useApp();
  const { isRawModeSupported } = useStdin();
  const { stdout } = useStdout();
  const interactive =
    isRawModeSupported &&
    Boolean(process.stdin.isTTY && process.stdin.setRawMode);
  const [mode, setMode] = useState<Mode>("inbox");
  const [agents, setAgents] = useState<AgentRow[]>([]);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [messages, setMessages] = useState<Record<string, Message[]>>({});
  const [broadcasts, setBroadcasts] = useState<Broadcast[]>([]);
  const [leased, setLeased] = useState<Message[]>([]);
  const [draft, setDraft] = useState("");
  const [notice, setNotice] = useState<Notice>({
    tone: "info",
    text: "connecting to relay",
  });
  const [busy, setBusy] = useState(false);
  const [lastUpdated, setLastUpdated] = useState("");
  const refreshId = useRef(0);

  const selectedAgent = agents[selectedIndex];
  const selectedMessages = selectedAgent
    ? (messages[selectedAgent.name] ?? [])
    : [];
  const onlineCount = agents.filter((agent) => agent.online).length;
  const width = Math.max(
    80,
    Math.min(stdout.columns || PANEL_WIDTH, PANEL_WIDTH),
  );

  const refresh = useCallback(async (quiet = false) => {
    const id = ++refreshId.current;
    try {
      const names = (await valkey.smembers(keys.agents())).sort();
      const online = new Set(await getOnlineAgents());
      const rows = await Promise.all(
        names.map(async (name) => ({
          name,
          online: online.has(name),
          messages: await getInboxCount(name),
        })),
      );
      const inboxes = await Promise.all(
        rows.map((row) => listInbox(row.name, 0, MAX_VISIBLE_MESSAGES + 20)),
      );
      const broadcasts = await listBroadcasts(0, 20);
      if (id !== refreshId.current) return;
      setAgents(rows);
      setMessages(
        Object.fromEntries(
          rows.map((row, index) => [row.name, inboxes[index].items]),
        ),
      );
      setBroadcasts(broadcasts.items);
      setSelectedIndex((current) =>
        Math.min(current, Math.max(0, rows.length - 1)),
      );
      setLastUpdated(shortTime(new Date().toISOString()));
      if (!quiet) setNotice({ tone: "good", text: "state synchronized" });
    } catch (error) {
      setNotice({
        tone: "critical",
        text: error instanceof Error ? error.message : "relay unavailable",
      });
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = setInterval(() => void refresh(true), 3000);
    return () => clearInterval(timer);
  }, [refresh]);

  const act = useCallback(
    async (operation: () => Promise<void>, success: string) => {
      if (busy) return;
      setBusy(true);
      try {
        await operation();
        setNotice({ tone: "good", text: success });
        await refresh(true);
      } catch (error) {
        setNotice({
          tone: "critical",
          text: error instanceof Error ? error.message : "operation failed",
        });
      } finally {
        setBusy(false);
      }
    },
    [busy, refresh],
  );

  const quit = useCallback(() => {
    onExit();
    exit();
  }, [exit, onExit]);

  const sendMessage = useCallback(() => {
    if (!selectedAgent || !draft.trim()) return;
    const body = draft.trim();
    setDraft("");
    setMode("inbox");
    void act(async () => {
      const message: Message = {
        id: crypto.randomUUID(),
        from: config.agentName,
        to: selectedAgent.name,
        body,
        timestamp: new Date().toISOString(),
      };
      const result = await appendMessage(message);
      if (result.created) {
        await publishEvent("msg_send", {
          from: message.from,
          to: message.to,
          messageId: message.id,
        });
        await valkey.publish(
          keys.notifyChannel(message.to),
          JSON.stringify(message),
        );
      }
    }, `message queued for ${selectedAgent.name}`);
  }, [act, config.agentName, draft, selectedAgent]);

  const sendBroadcast = useCallback(() => {
    if (!draft.trim()) return;
    const body = draft.trim();
    setDraft("");
    setMode("broadcasts");
    void act(async () => {
      const broadcast: Broadcast = {
        id: crypto.randomUUID(),
        from: config.agentName,
        body,
        timestamp: new Date().toISOString(),
      };
      const result = await appendBroadcast(broadcast);
      if (result.created)
        await publishEvent("broadcast", {
          from: broadcast.from,
          messageId: broadcast.id,
        });
    }, "broadcast delivered to all agents");
  }, [act, config.agentName, draft]);

  const leaseSelected = useCallback(() => {
    if (!selectedAgent) return;
    void act(async () => {
      const batch = await leaseMessages(selectedAgent.name, instanceId, 10);
      setLeased(batch);
      if (batch.length === 0) throw new Error("no unleased messages available");
    }, `${selectedAgent.name}: messages leased`);
  }, [act, instanceId, selectedAgent]);

  const ackSelected = useCallback(() => {
    if (!selectedAgent || leased.length === 0) return;
    void act(async () => {
      const result = await acknowledgeMessages(
        selectedAgent.name,
        instanceId,
        leased.map((message) => message.id),
      );
      if (result.rejected.length > 0)
        throw new Error(`${result.rejected.length} receipts rejected`);
      setLeased([]);
    }, `${selectedAgent.name}: ${leased.length} messages acknowledged`);
  }, [act, instanceId, leased, selectedAgent]);

  useInput(
    (input, key) => {
      if (key.ctrl && input === "c") return quit();
      if (mode === "compose-message" || mode === "compose-broadcast") {
        if (key.escape)
          return setMode(mode === "compose-message" ? "inbox" : "broadcasts");
        if (key.return)
          return mode === "compose-message" ? sendMessage() : sendBroadcast();
        if (key.backspace || key.delete)
          return setDraft((value) => value.slice(0, -1));
        if (key.ctrl && input === "u") return setDraft("");
        if (input && !key.ctrl && !key.meta) setDraft((value) => value + input);
        return;
      }
      if (input === "q") return quit();
      if (input === "r") return void refresh();
      if (input === "?") return setMode("help");
      if (input === "i") return setMode("inbox");
      if (input === "b")
        return setMode(
          mode === "broadcasts" ? "compose-broadcast" : "broadcasts",
        );
      if (input === "m") return setMode("compose-message");
      if (input === "l") return leaseSelected();
      if (input === "a") return ackSelected();
      if (key.upArrow || input === "k")
        return setSelectedIndex((index) => Math.max(0, index - 1));
      if (key.downArrow || input === "j")
        return setSelectedIndex((index) =>
          Math.min(Math.max(0, agents.length - 1), index + 1),
        );
    },
    { isActive: interactive },
  );

  const body =
    mode === "inbox" ? (
      <InboxView
        agent={selectedAgent}
        messages={selectedMessages}
        leased={leased}
        busy={busy}
      />
    ) : mode === "broadcasts" ? (
      <BroadcastView broadcasts={broadcasts} />
    ) : mode === "help" ? (
      <HelpView />
    ) : (
      <ComposeView
        mode={mode}
        from={config.agentName}
        target={selectedAgent?.name}
        draft={draft}
        busy={busy}
      />
    );

  return (
    <Box width={width} flexDirection="column" paddingY={1}>
      <Header
        config={config}
        mode={mode}
        onlineCount={onlineCount}
        lastUpdated={lastUpdated}
      />
      <Tabs mode={mode} />
      <Box marginTop={1} gap={1} height={26}>
        <AgentRail
          agents={agents}
          selectedIndex={selectedIndex}
          onSelect={(index) => {
            setSelectedIndex(index);
            setMode("inbox");
          }}
        />
        {body}
      </Box>
      <Footer notice={notice} />
      {!interactive && (
        <Text color={COLORS.warning}>
          Interactive input requires a TTY. Run `npx postino tui` from a
          terminal.
        </Text>
      )}
    </Box>
  );
}

export async function runTui(): Promise<void> {
  const config = (await import("./types.js")).loadConfig();
  if (!process.stdin.isTTY || !process.stdin.setRawMode) {
    process.stderr.write("postino tui: interactive input requires a TTY\n");
    return;
  }
  const instanceId = crypto.randomUUID();
  try {
    await connect();
    await registerAgent(config.agentName, instanceId);
    const { waitUntilExit } = render(
      <TuiApp config={config} instanceId={instanceId} onExit={() => {}} />,
    );
    await waitUntilExit();
  } finally {
    await deregisterAgent(config.agentName, instanceId).catch(() => {});
    await disconnect().catch(() => {});
  }
}
