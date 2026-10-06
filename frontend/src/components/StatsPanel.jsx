export default function StatsPanel({ feed, connected }) {
  const total = feed.length;
  const flaggedCount = feed.filter((f) => f.flagged).length;
  const fraudRate = total > 0 ? ((flaggedCount / total) * 100).toFixed(2) : "0.00";

  return (
    <div style={{ display: "flex", gap: 24, marginBottom: 16 }}>
      <Stat label="Status" value={connected ? "Connected" : "Reconnecting…"} />
      <Stat label="Transactions seen" value={total} />
      <Stat label="Flagged" value={flaggedCount} />
      <Stat label="Fraud rate" value={`${fraudRate}%`} />
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div>
      <div style={{ fontSize: 12, color: "#888" }}>{label}</div>
      <div style={{ fontSize: 20, fontWeight: 600 }}>{value}</div>
    </div>
  );
}