import { ingest } from './ingest.js';

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(ingest(env));
    const scheduledAt = new Date(event.scheduledTime);
    if (scheduledAt.getUTCHours() === 11 && scheduledAt.getUTCMinutes() === 0) {
      ctx.waitUntil(env.DB.batch([
        env.DB.prepare("DELETE FROM kills WHERE occurred_at < datetime('now','-2 days')"),
        env.DB.prepare("DELETE FROM activity_hour WHERE hour_utc < datetime('now','-6 months')"),
      ]));
    }
  },
  async fetch() {
    return new Response('EVE killmail collector active. Public website temporarily disabled.', {
      status: 503,
      headers: { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' },
    });
  },
};
