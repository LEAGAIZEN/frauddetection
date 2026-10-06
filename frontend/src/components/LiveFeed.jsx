export default function LiveFeed({ feed }) {
  if (feed.length === 0) {
    return <p style={{ color: "#888" }}>Waiting for transactions…</p>;
  }

  return (
    <div style={{ maxHeight: 400, overflowY: "auto" }}>
      {feed.map((item) => (
        <div
          key={item.transaction_id}
          style={{
            display: "flex",
            justifyContent: "space-between",
            padding: "8px 12px",
            marginBottom: 4,
            borderRadius: 6,
            background: item.flagged ? "#fde2e2" : "#f4f4f4",
            borderLeft: item.flagged ? "4px solid #d9534f" : "4px solid #ccc",
          }}
        >
          <span>#{item.transaction_id}</span>
          <span>${item.amount.toFixed(2)}</span>
          <span>{(item.risk_score * 100).toFixed(1)}%</span>
          <span>{item.flagged ? "FRAUD" : "ok"}</span>
        </div>
      ))}
    </div>
  );
}