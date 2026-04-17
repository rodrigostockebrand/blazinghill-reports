# BlazingHill Reports — Railway Deployment Guide

## Repository
`rodrigostockebrand/blazinghill-reports` on GitHub

## Prerequisites
- Railway account linked to GitHub
- All API keys below

## Environment Variables

Set these in Railway's service settings:

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENAI_API_KEY` | Yes | GPT report generation (gpt-4.1) |
| `PERPLEXITY_API_KEY` | Yes | Research phase data collection |
| `CASHMERE_API_KEY` | Optional | PitchBook/CB Insights/Statista premium data |
| `DATAFORSEO_LOGIN` | Optional | DataForSEO API — search volume, traffic, sentiment |
| `DATAFORSEO_PASSWORD` | Optional | DataForSEO API password |
| `AHREFS_API_KEY` | Optional | Backlink and domain authority data |
| `JWT_SECRET` | Yes | Auth tokens — generate with `openssl rand -hex 32` |
| `NODE_ENV` | Yes | Set to `production` |
| `PORT` | Auto | Railway sets this automatically (default 8000) |

## Volume Mount

Mount a persistent volume at `/data` for:
- SQLite database (`/data/blazinghill.db`)
- Generated reports (`/data/reports/`)

In Railway: Settings > Volumes > Add Volume > Mount path: `/data`

## Deploy Steps

1. **Connect repo**: In Railway, create new project > Deploy from GitHub > select `rodrigostockebrand/blazinghill-reports`
2. **Set env vars**: Add all variables from the table above
3. **Add volume**: Mount persistent volume at `/data`
4. **Deploy**: Railway auto-detects the Dockerfile and builds

## Build Configuration

The repo includes:
- `Dockerfile` — multi-stage build (Node.js + Python)
- `railway.json` — Railway-specific config
- `nixpacks.toml` — Nixpacks fallback config

Railway will use the Dockerfile by default.

## Health Check

The server exposes `GET /api/health` on port 8000.

## Post-Deploy Verification

1. Visit the Railway-provided URL
2. Login or create an account
3. Generate a test report to verify all APIs are working
4. Check logs for any missing API key warnings

## Troubleshooting

- **Report generation fails**: Check `OPENAI_API_KEY` and `PERPLEXITY_API_KEY` are set
- **No premium data**: Ensure `CASHMERE_API_KEY` is set for PitchBook/Statista/CB Insights
- **No traffic/SEO charts**: Set `DATAFORSEO_LOGIN` and `DATAFORSEO_PASSWORD`
- **Auth errors**: Verify `JWT_SECRET` is set
- **DB lost on redeploy**: Ensure volume is mounted at `/data`
