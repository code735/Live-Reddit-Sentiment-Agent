export default function Home() {
  const stats = [
    { label: "Avg sentiment", value: "+0.42", delta: "+0.06", tone: "up" },
    { label: "Active streams", value: "18", delta: "+3", tone: "up" },
    { label: "Mentions / min", value: "1,240", delta: "-4%", tone: "down" },
    { label: "Toxicity flags", value: "12", delta: "-2", tone: "up" },
  ];

  const pulse = [
    { label: "Momentum", value: "Steady", detail: "3h average" },
    { label: "Signal", value: "Positive", detail: "Tech & finance" },
    { label: "Noise", value: "Low", detail: "8% volatility" },
  ];

  const subreddits = [
    { name: "r/technology", sentiment: "+0.58", mentions: "1.2k", change: "+12%" },
    { name: "r/fintech", sentiment: "+0.44", mentions: "860", change: "+8%" },
    { name: "r/stocks", sentiment: "+0.31", mentions: "740", change: "+6%" },
    { name: "r/startups", sentiment: "+0.27", mentions: "620", change: "+4%" },
  ];

  const posts = [
    { title: "AI earnings beat expectations across Q4", score: 0.72, time: "3m" },
    { title: "Debate heats up on rate cuts and housing", score: 0.18, time: "12m" },
    { title: "Open source models gain traction in Europe", score: 0.41, time: "24m" },
  ];

  return (
    <div className="relative min-h-screen overflow-hidden px-6 py-10 sm:px-10">
      <div className="pointer-events-none absolute -left-40 top-10 h-72 w-72 rounded-full bg-[radial-gradient(circle,#f4a261,transparent_70%)] opacity-60" />
      <div className="pointer-events-none absolute -right-32 bottom-10 h-80 w-80 rounded-full bg-[radial-gradient(circle,#2a9d8f,transparent_70%)] opacity-50" />

      <main className="relative mx-auto flex w-full max-w-6xl flex-col gap-10">
        <header className="flex flex-col gap-6 sm:flex-row sm:items-center sm:justify-between">
          <div className="space-y-2">
            <p className="text-sm font-medium uppercase tracking-[0.3em] text-[color:var(--accent)]">
              Live Reddit Sentiment Agent
            </p>
            <h1 className="text-3xl font-semibold leading-tight text-[color:var(--foreground)] sm:text-4xl">
              Market pulse dashboard
            </h1>
            <p className="max-w-xl text-base text-slate-600">
              Track realtime sentiment across high-impact subreddits with trends, volume, and volatility signals.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <button className="rounded-full border border-[color:var(--muted)] bg-[color:var(--surface)] px-5 py-2 text-sm font-medium shadow-[var(--shadow)] transition hover:-translate-y-0.5 hover:shadow-lg">
              Configure sources
            </button>
            <button className="rounded-full bg-[linear-gradient(120deg,#f4a261,#e07a5f,#2a9d8f)] px-5 py-2 text-sm font-semibold text-white shadow-[var(--shadow)] [background-size:200%_200%] [animation:shimmer_6s_ease_infinite]">
              Export snapshot
            </button>
          </div>
        </header>

        <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {stats.map((stat, index) => (
            <div
              key={stat.label}
              className="rounded-3xl border border-[color:var(--muted)] bg-[color:var(--surface)] p-5 shadow-[var(--shadow)] [animation:floatUp_0.6s_ease-out]"
              style={{ animationDelay: `${index * 90}ms` }}
            >
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500">
                {stat.label}
              </p>
              <div className="mt-4 flex items-baseline justify-between">
                <span className="text-2xl font-semibold text-[color:var(--foreground)]">
                  {stat.value}
                </span>
                <span
                  className={`text-sm font-semibold ${
                    stat.tone === "down" ? "text-rose-500" : "text-emerald-600"
                  }`}
                >
                  {stat.delta}
                </span>
              </div>
            </div>
          ))}
        </section>

        <section className="grid gap-8 lg:grid-cols-[1.2fr_0.8fr]">
          <div className="rounded-[32px] border border-[color:var(--muted)] bg-[color:var(--surface)] p-7 shadow-[var(--shadow)]">
            <div className="flex flex-col gap-6 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <h2 className="text-xl font-semibold text-[color:var(--foreground)]">Live pulse</h2>
                <p className="text-sm text-slate-500">
                  Rolling 24h signal for market-moving topics.
                </p>
              </div>
              <div className="rounded-full border border-[color:var(--muted)] bg-[color:var(--muted)] px-4 py-1 text-xs font-semibold text-slate-700">
                Updated 2m ago
              </div>
            </div>

            <div className="mt-6 grid gap-4 sm:grid-cols-3">
              {pulse.map((item) => (
                <div
                  key={item.label}
                  className="rounded-2xl border border-[color:var(--muted)] bg-[color:var(--surface)] p-4"
                >
                  <p className="text-xs uppercase tracking-[0.2em] text-slate-500">
                    {item.label}
                  </p>
                  <p className="mt-3 text-lg font-semibold text-[color:var(--foreground)]">
                    {item.value}
                  </p>
                  <p className="text-xs text-slate-500">{item.detail}</p>
                </div>
              ))}
            </div>

            <div className="mt-6 rounded-3xl border border-dashed border-[color:var(--muted)] bg-[linear-gradient(120deg,rgba(244,162,97,0.25),rgba(224,122,95,0.12),rgba(42,157,143,0.2))] p-6 [background-size:200%_200%] [animation:shimmer_10s_ease_infinite]">
              <div className="flex items-center justify-between">
                <p className="text-sm font-medium text-slate-700">Sentiment trendline</p>
                <span className="text-xs font-semibold text-emerald-600">+8.4% week</span>
              </div>
              <div className="mt-6 grid h-32 grid-cols-12 items-end gap-2">
                {[20, 35, 28, 45, 32, 58, 44, 64, 52, 70, 62, 78].map((height, idx) => (
                  <div
                    key={height}
                    className="rounded-full bg-[color:var(--accent-2)]/80"
                    style={{ height: `${height}%`, opacity: 0.6 + idx * 0.03 }}
                  />
                ))}
              </div>
            </div>
          </div>

          <div className="flex flex-col gap-6">
            <div className="rounded-[28px] border border-[color:var(--muted)] bg-[color:var(--surface)] p-6 shadow-[var(--shadow)]">
              <h3 className="text-lg font-semibold text-[color:var(--foreground)]">Top subreddits</h3>
              <p className="text-sm text-slate-500">Highest impact on sentiment shift.</p>
              <div className="mt-6 space-y-4">
                {subreddits.map((item) => (
                  <div key={item.name} className="flex items-center justify-between">
                    <div>
                      <p className="text-sm font-semibold text-[color:var(--foreground)]">
                        {item.name}
                      </p>
                      <p className="text-xs text-slate-500">{item.mentions} mentions</p>
                    </div>
                    <div className="text-right">
                      <p className="text-sm font-semibold text-emerald-600">
                        {item.sentiment}
                      </p>
                      <p className="text-xs text-slate-500">{item.change}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="rounded-[28px] border border-[color:var(--muted)] bg-[color:var(--surface)] p-6 shadow-[var(--shadow)]">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold text-[color:var(--foreground)]">Recent posts</h3>
                <span className="rounded-full bg-[color:var(--muted)] px-3 py-1 text-xs font-semibold text-slate-600">
                  Live feed
                </span>
              </div>
              <div className="mt-6 space-y-4">
                {posts.map((post) => (
                  <div
                    key={post.title}
                    className="rounded-2xl border border-[color:var(--muted)] bg-[color:var(--surface)] p-4 [animation:fadeIn_0.8s_ease-out]"
                  >
                    <p className="text-sm font-semibold text-[color:var(--foreground)]">
                      {post.title}
                    </p>
                    <div className="mt-3 flex items-center justify-between text-xs text-slate-500">
                      <span className="font-mono">sentiment {post.score.toFixed(2)}</span>
                      <span>{post.time} ago</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>
      </main>
    </div>
  );
}
