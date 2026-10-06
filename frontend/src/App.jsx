import { useLiveFeed } from "./hooks/useWebSocket";
import StatsPanel from "./components/StatsPanel";
import LiveFeed from "./components/LiveFeed";

export default function App() {
  const { feed, connected } = useLiveFeed();

  return (
    <div style={{ fontFamily: "sans-serif", padding: 24, maxWidth: 700, margin: "0 auto" }}>
      <h1>Fraud Detection Dashboard</h1>
      <StatsPanel feed={feed} connected={connected} />
      <LiveFeed feed={feed} />
    </div>
  );
}