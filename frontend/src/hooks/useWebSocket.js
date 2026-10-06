import { useEffect, useRef, useState } from "react";

const WS_URL = "ws://localhost:8000/ws";
const MAX_FEED_LENGTH = 100;

export function useLiveFeed() {
  const [feed, setFeed] = useState([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);

  useEffect(() => {
    let cancelled = false;

    function connect() {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (!cancelled) setTimeout(connect, 2000);
      };
      ws.onerror = () => ws.close();
      ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        setFeed((prev) => [data, ...prev].slice(0, MAX_FEED_LENGTH));
      };
    }

    connect();
    return () => {
      cancelled = true;
      wsRef.current?.close();
    };
  }, []);

  return { feed, connected };
}