'use strict';
/**
 * NAMEKTUKAM downloader backend (Express + yt-dlp)
 * Alur: POST /api/dl/start -> GET /api/dl/progress/:id (SSE) -> GET /api/dl/file/:id
 */
const express = require('express');
const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const YTDLP = process.env.YTDLP_PATH || 'yt-dlp';
const MAX_FILE_MB = Number(process.env.MAX_FILE_MB) || 500;
const JOB_TTL_MS = 15 * 60 * 1000;      // File dihapus 15 menit setelah dibuat
const JOB_TIMEOUT_MS = 5 * 60 * 1000;   // Batas waktu unduhan (5 menit)
const MAX_ACTIVE_PER_IP = 2;
const MAX_STARTS_PER_MIN = 6;
const ALLOWED_HOSTS = (process.env.ALLOWED_HOSTS ||
  'youtube.com,youtu.be,tiktok.com,instagram.com,facebook.com,fb.watch,twitter.com,x.com,vimeo.com,soundcloud.com')
  .split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '10kb' }));

// CORS Header
app.use('/api/dl', (req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', process.env.ALLOWED_ORIGIN || '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const jobs = new Map();
const startLog = new Map();

const hostAllowed = u => {
  const h = u.hostname.toLowerCase();
  return ALLOWED_HOSTS.some(a => h === a || h.endsWith('.' + a));
};

const snapshot = j => ({
  id: j.id,
  status: j.status,
  percent: Math.round(j.percent),
  text: j.text,
  title: j.title,
  filename: j.filename,
  error: j.error
});

function publish(job) {
  const payload = `data: ${JSON.stringify(snapshot(job))}\n\n`;
  for (const r of job.clients) r.write(payload);
  if (job.status === 'done' || job.status === 'error') {
    for (const r of job.clients) r.end();
    job.clients.clear();
  }
}

function fail(job, msg) {
  if (job.status === 'done' || job.status === 'error') return;
  job.status = 'error';
  job.error = msg;
  job.text = 'Gagal';
  publish(job);
}

function friendlyError(stderr) {
  if (/larger than|max-filesize/i.test(stderr)) return `Ukuran file melebihi batas ${MAX_FILE_MB} MB.`;
  if (/private|sign in|log in|login|cookies|age/i.test(stderr)) return 'Video privat, dibatasi umur, atau butuh login.';
  if (/unsupported url/i.test(stderr)) return 'Link tidak didukung.';
  if (/ffmpeg|ffprobe/i.test(stderr)) return 'ffmpeg belum terpasang di server.';
  return 'Gagal mengunduh. Cek link-nya atau coba lagi nanti.';
}

function handleLine(job, expectedStreams, line) {
  if (line.startsWith('TITLE:')) {
    job.title = line.slice(6).trim().slice(0, 150);
    job.text = 'Mengunduh...';
    publish(job);
  } else if (line.startsWith('PROG:')) {
    const p = parseFloat(line.slice(5));
    if (Number.isNaN(p)) return;
    if (p < job.lastP - 50) job.stream++;
    job.lastP = p;
    const overall = Math.min(99, ((job.stream + p / 100) / expectedStreams) * 100);
    job.percent = Math.max(job.percent, overall);
    job.text = p >= 100 && job.stream + 1 >= expectedStreams ? 'Memproses file...' : 'Mengunduh...';
    publish(job);
  }
}

// Start Download
app.post('/api/dl/start', (req, res) => {
  const { url, format } = req.body || {};
  let u;
  try { u = new URL(String(url)); } catch { return res.status(400).json({ error: 'URL tidak valid.' }); }
  if (!['http:', 'https:'].includes(u.protocol)) return res.status(400).json({ error: 'URL tidak valid.' });
  if (!hostAllowed(u)) return res.status(400).json({ error: 'Situs ini belum didukung.' });
  if (!['mp4', 'mp3'].includes(format)) return res.status(400).json({ error: 'Format harus mp4 atau mp3.' });

  const ip = req.ip || req.socket.remoteAddress;
  const now = Date.now();

  const userStarts = (startLog.get(ip) || []).filter(t => now - t < 60000);
  if (userStarts.length >= MAX_STARTS_PER_MIN) return res.status(429).json({ error: 'Terlalu banyak permintaan. Coba sebentar lagi.' });
  userStarts.push(now);
  startLog.set(ip, userStarts);

  const activeJobs = Array.from(jobs.values()).filter(j => j.ip === ip && (j.status === 'pending' || j.status === 'downloading'));
  if (activeJobs.length >= MAX_ACTIVE_PER_IP) return res.status(429).json({ error: 'Proses unduhan kamu yang lain masih berjalan.' });

  const id = crypto.randomBytes(8).toString('hex');
  const tmpDir = path.join(os.tmpdir(), `dl-${id}`);
  fs.mkdirSync(tmpDir, { recursive: true });

  const job = {
    id, ip, url: u.href, format, tmpDir,
    status: 'pending', percent: 0, lastP: 0, stream: 0,
    title: 'Menyiapkan...', text: 'Menghubungkan...', filename: null, error: null,
    clients: new Set()
  };
  jobs.set(id, job);

  const isMp3 = format === 'mp3';
  const expectedStreams = isMp3 ? 1 : 2;
  const outTmpl = path.join(tmpDir, '%(title).100s.%(ext)s');

  const args = [
    '--no-playlist', '--no-warnings', '--newline',
    '--max-filesize', `${MAX_FILE_MB}M`,
    '--print', 'TITLE:%(title)s',
    '--progress-template', 'PROG:%(progress._percent_str)s',
    '-o', outTmpl
  ];

  if (isMp3) {
    args.push('-x', '--audio-format', 'mp3', '--audio-quality', '0');
  } else {
    args.push('-f', 'bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/b', '--merge-output-format', 'mp4');
  }
  args.push(job.url);

  job.status = 'downloading';
  const proc = spawn(YTDLP, args);
  let stderr = '';

  const timer = setTimeout(() => {
    proc.kill('SIGKILL');
    fail(job, 'Waktu mengunduh habis (Timeout).');
  }, JOB_TIMEOUT_MS);

  proc.stdout.on('data', data => {
    const lines = data.toString().split('\n');
    for (const l of lines) handleLine(job, expectedStreams, l.trim());
  });

  proc.stderr.on('data', data => { stderr += data.toString(); });

  proc.on('close', code => {
    clearTimeout(timer);
    if (job.status === 'error') return;

    if (code !== 0) {
      return fail(job, friendlyError(stderr));
    }

    try {
      const files = fs.readdirSync(tmpDir).filter(f => !f.endsWith('.part') && !f.endsWith('.ytdl'));
      if (!files.length) return fail(job, 'File hasil unduhan tidak ditemukan.');

      job.filename = files[0];
      job.percent = 100;
      job.status = 'done';
      job.text = 'Selesai!';
      publish(job);

      setTimeout(() => {
        fs.rm(tmpDir, { recursive: true, force: true }, () => {});
        jobs.delete(id);
      }, JOB_TTL_MS);

    } catch {
      fail(job, 'Gagal memproses file akhir.');
    }
  });

  res.json({ id });
});

// Real-time Progress (SSE)
app.get('/api/dl/progress/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'Sesi tidak ditemukan.' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  job.clients.add(res);
  res.write(`data: ${JSON.stringify(snapshot(job))}\n\n`);

  req.on('close', () => job.clients.delete(res));
});

// Download File Endpoint
app.get('/api/dl/file/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job || job.status !== 'done' || !job.filename) {
    return res.status(404).send('File tidak ditemukan atau belum selesai diunduh.');
  }

  const filePath = path.join(job.tmpDir, job.filename);
  if (!fs.existsSync(filePath)) return res.status(404).send('File sudah kadaluwarsa.');

  res.download(filePath, job.filename);
});

// ================= HTML ORI (DENGAN FITUR DOWNLOADER ASLI) =================
const HTML_CONTENT = `<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>NAMEKTUKAM — Creator Dashboard</title>
<meta name="description" content="Dashboard NAMEKTUKAM: project tracker, preset library, tools editor, YTTA tools, downloader, dan panel owner." />
<meta http-equiv="Content-Security-Policy" content="default-src 'self'; script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdnjs.cloudflare.com; font-src 'self' https://fonts.gstatic.com https://cdnjs.cloudflare.com; img-src 'self' data: https://img.youtube.com https://*.ytimg.com https://m.media-amazon.com; media-src 'self' https: http:; connect-src 'self' https://am.dapjisync.my.id https://generativelanguage.googleapis.com http://localhost:3000; frame-src 'self'; frame-ancestors 'none';" />
<meta name="referrer" content="strict-origin-when-cross-origin" />
<meta http-equiv="X-Content-Type-Options" content="nosniff" />
<script>
  if (window.top !== window.self) {
    window.top.location = window.self.location;
  }
</script>
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" />
<style>
  * { margin:0; padding:0; box-sizing:border-box; -webkit-tap-highlight-color:transparent; }
  :root {
    --text:#f5f5f7; --text-dim:rgba(245,245,247,.65); --text-mute:rgba(245,245,247,.42);
    --green:#4ade80; --cyan:#67e8f9; --red:#f87171; --yellow:#fbbf24;
  }
  html,body { background:#000; color:var(--text); font-family:'Inter',-apple-system,'Segoe UI',sans-serif; -webkit-font-smoothing:antialiased; overflow-x:hidden; min-height:100vh; }
  .bg-layer { position:fixed; inset:0; z-index:-2;
    background:radial-gradient(ellipse 80% 60% at 15% 10%,rgba(120,80,255,.28),transparent 60%),
    radial-gradient(ellipse 70% 55% at 85% 20%,rgba(0,200,255,.2),transparent 60%),
    radial-gradient(ellipse 90% 70% at 50% 100%,rgba(255,60,180,.18),transparent 60%),#000;
    animation:floatBg 20s ease-in-out infinite alternate; }
  @keyframes floatBg { to { transform:scale(1.08) translate(-1%,-1%); } }
  .hidden { display:none !important; }
  .mono { font-family:'JetBrains Mono',monospace; }
  nav { position:fixed; top:12px; left:50%; transform:translateX(-50%); width:calc(100% - 24px); max-width:1180px; z-index:100;
    display:flex; flex-direction:column; align-items:center; gap:10px; padding:12px; border-radius:20px;
    background:linear-gradient(180deg,rgba(255,255,255,.10),rgba(255,255,255,.04));
    backdrop-filter:blur(24px) saturate(180%); -webkit-backdrop-filter:blur(24px) saturate(180%);
    border:1px solid rgba(255,255,255,.14); box-shadow:0 10px 40px -8px rgba(0,0,0,.6); }
  @media(min-width:900px){ nav { flex-direction:row; justify-content:space-between; padding:10px 14px 10px 22px; top:18px; } }
  .brand { display:flex; align-items:center; gap:11px; font-weight:700; font-size:16px; }
  .brand-logo { width:30px; height:30px; border-radius:10px; display:grid; place-items:center; background:linear-gradient(135deg,rgba(255,255,255,.22),rgba(255,255,255,.06)); border:1px solid rgba(255,255,255,.22); }
  .brand b { color:rgba(255,255,255,.55); font-weight:500; }
  .nav-right { display:flex; align-items:center; gap:10px; width:100%; }
  @media(min-width:900px){ .nav-right { width:auto; } }
  .nav-tabs { position:relative; display:flex; flex:1; background:rgba(0,0,0,.45); padding:4px; border-radius:14px; border:1px solid rgba(255,255,255,.08); overflow-x:auto; scrollbar-width:none; }
  .nav-tabs::-webkit-scrollbar { display:none; }
  .tab-btn { position:relative; z-index:2; padding:7px 12px; border-radius:10px; font-size:11.5px; font-weight:600; color:var(--text-dim); background:transparent; border:none; cursor:pointer; transition:color .3s; display:flex; align-items:center; gap:5px; white-space:nowrap; flex-shrink:0; }
  .tab-btn:hover { color:var(--text); } .tab-btn.active { color:#000; }
  .liquid-pill { position:absolute; top:4px; height:calc(100% - 8px); border-radius:10px; z-index:1; background:linear-gradient(135deg,#fff,#e0e0e0); box-shadow:0 4px 15px rgba(255,255,255,.3); transition:all .35s cubic-bezier(.25,1,.3,1); }
  .user-chip { font-size:11px; color:var(--text-dim); white-space:nowrap; padding:6px 10px; border:1px solid rgba(255,255,255,.12); border-radius:20px; }
  main { padding:135px 16px 60px; max-width:1180px; margin:0 auto; }
  @media(min-width:900px){ main { padding-top:110px; } }
  .auth-wrap { min-height:100vh; display:grid; place-items:center; padding:24px 16px; }
  .tab-content { display:none; }
  .tab-content.active { display:block; animation:fade .4s ease; }
  @keyframes fade { from { opacity:0; transform:translateY(10px); } to { opacity:1; transform:none; } }
  .card { width:100%; max-width:480px; margin:0 auto; padding:26px 22px 24px; border-radius:24px; background:linear-gradient(160deg,rgba(255,255,255,.09),rgba(255,255,255,.025)); backdrop-filter:blur(32px); -webkit-backdrop-filter:blur(32px); border:1px solid rgba(255,255,255,.14); box-shadow:0 30px 80px -20px rgba(0,0,0,.85); }
  .card-wide { max-width:1100px; }
  .head { text-align:center; margin-bottom:20px; }
  .head-logo { width:50px; height:50px; border-radius:16px; margin:0 auto 14px; display:grid; place-items:center; font-size:22px; background:linear-gradient(135deg,rgba(255,255,255,.2),rgba(255,255,255,.05)); border:1px solid rgba(255,255,255,.2); }
  .head h1 { font-size:20px; font-weight:700; margin-bottom:6px; }
  .head p { font-size:12.5px; color:var(--text-dim); line-height:1.5; }
  .grid2 { display:grid; grid-template-columns:1fr; gap:16px; }
  @media(min-width:800px){ .grid2 { grid-template-columns:1fr 1fr; } }
  .grid3 { display:grid; grid-template-columns:1fr; gap:12px; }
  @media(min-width:800px){ .grid3 { grid-template-columns:repeat(3,1fr); } }
  .stats-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:12px; margin-bottom:20px; }
  .stat-card { background:rgba(255,255,255,.04); border:1px solid rgba(255,255,255,.1); border-radius:14px; padding:14px; }
  .stat-card .label { font-size:11px; color:var(--text-mute); margin-bottom:4px; }
  .stat-card .val { font-size:22px; font-weight:700; }
  .sec { background:rgba(0,0,0,.3); border:1px solid rgba(255,255,255,.08); border-radius:16px; padding:16px; margin-bottom:16px; }
  .sec h3 { font-size:13.5px; font-weight:700; margin-bottom:12px; color:var(--cyan); }
  .muted { font-size:11.5px; color:var(--text-dim); line-height:1.6; }
  .banner { margin:0 auto 16px; max-width:1100px; padding:10px 16px; border-radius:14px; background:rgba(251,191,36,.12); border:1px solid rgba(251,191,36,.35); color:var(--yellow); font-size:12.5px; display:flex; justify-content:space-between; gap:10px; align-items:center; }
  .msg-box { display:none; padding:10px 12px; border-radius:12px; margin-bottom:12px; font-size:12px; line-height:1.4; }
  .msg-box.show { display:flex; align-items:center; gap:8px; }
  .msg-box.error { background:rgba(248,113,113,.15); border:1px solid rgba(248,113,113,.4); color:var(--red); }
  .msg-box.success { background:rgba(74,222,128,.15); border:1px solid rgba(74,222,128,.4); color:var(--green); }
  .bulk-item { display:flex; align-items:center; justify-content:space-between; gap:10px; background:rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.1); padding:8px 12px; border-radius:10px; margin-top:8px; font-size:12px; }
  .bulk-item-num { font-weight:700; color:var(--cyan); }
  .bulk-item-info { flex:1; overflow:hidden; }
  .bulk-item-email { font-weight:600; text-overflow:ellipsis; overflow:hidden; white-space:nowrap; }
  .bulk-item-link { font-size:11px; color:var(--cyan); text-decoration:none; }
  .bulk-item-link:hover { text-decoration:underline; }
  .bulk-item-status.ok { color:var(--green); }
  .bulk-item-status.fail { color:var(--red); }
  form { display:flex; flex-direction:column; gap:12px; }
  .field { display:flex; flex-direction:column; gap:6px; }
  .field label { font-size:12px; font-weight:500; color:var(--text-dim); }
  .row { display:flex; gap:8px; flex-wrap:wrap; }
  .row > * { flex:1; min-width:100px; }
  input,textarea,select { width:100%; padding:11px 13px; border-radius:12px; background:rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.12); color:var(--text); font-size:13px; font-family:inherit; outline:none; transition:.25s; }
  input:focus,textarea:focus,select:focus { border-color:rgba(255,255,255,.35); box-shadow:0 0 15px rgba(255,255,255,.12); }
  select option { background:#111; }
  textarea { resize:vertical; min-height:90px; }
  input[type=color] { padding:3px; height:42px; cursor:pointer; }
  .btn { display:inline-flex; align-items:center; justify-content:center; gap:8px; padding:11px 16px; border-radius:12px; font-size:13px; font-weight:600; cursor:pointer; border:none; transition:.28s cubic-bezier(.16,1,.3,1); width:100%; text-decoration:none; }
  .btn-primary { color:#0a0a0a; background:#fff; } .btn-primary:hover { transform:translateY(-2px); box-shadow:0 10px 25px rgba(255,255,255,.3); }
  .btn-glass { color:var(--text); background:rgba(255,255,255,.08); border:1px solid rgba(255,255,255,.14); } .btn-glass:hover { background:rgba(255,255,255,.16); }
  .btn-danger { color:#fff; background:rgba(248,113,113,.2); border:1px solid rgba(248,113,113,.4); } .btn-danger:hover { background:rgba(248,113,113,.4); }
  .btn-sm { width:auto; padding:5px 10px; font-size:11px; border-radius:9px; }
  .switch-auth { margin-top:14px; text-align:center; font-size:12px; color:var(--text-dim); }
  .switch-auth a { color:#fff; font-weight:600; text-decoration:underline; cursor:pointer; }
  .table-wrap { background:rgba(0,0,0,.4); border:1px solid rgba(255,255,255,.1); border-radius:14px; overflow-x:auto; margin-top:10px; }
  table { width:100%; border-collapse:collapse; font-size:12px; text-align:left; min-width:480px; }
  th { background:rgba(255,255,255,.06); padding:10px 14px; color:var(--text-dim); font-weight:600; }
  td { padding:10px 14px; border-top:1px solid rgba(255,255,255,.05); }
  .badge { padding:2px 8px; border-radius:20px; font-size:9.5px; font-weight:600; display:inline-block; }
  .b-green { background:rgba(74,222,128,.2); color:var(--green); border:1px solid rgba(74,222,128,.4); }
  .b-red { background:rgba(248,113,113,.2); color:var(--red); border:1px solid rgba(248,113,113,.4); }
  .kanban { display:grid; grid-template-columns:1fr; gap:12px; margin-top:12px; }
  @media(min-width:800px){ .kanban { grid-template-columns:repeat(3,1fr); } }
  .col { background:rgba(0,0,0,.3); border:1px solid rgba(255,255,255,.08); border-radius:14px; padding:10px; min-height:90px; }
  .col h4 { font-size:11.5px; color:var(--text-dim); margin-bottom:8px; }
  .item { background:rgba(255,255,255,.06); border:1px solid rgba(255,255,255,.1); border-radius:10px; padding:9px 10px; margin-bottom:8px; font-size:12px; }
  .item small { display:block; color:var(--text-mute); margin-top:2px; font-size:10.5px; }
  .item .acts { display:flex; gap:5px; margin-top:8px; }
  .preset { background:rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.1); border-radius:12px; padding:12px; margin-bottom:10px; }
  .preset pre { font-family:'JetBrains Mono',monospace; font-size:11px; color:var(--text-dim); white-space:pre-wrap; word-break:break-word; margin:8px 0; max-height:110px; overflow:auto; }
  .out { font-family:'JetBrains Mono',monospace; font-size:12px; background:rgba(0,0,0,.4); border:1px solid rgba(255,255,255,.1); border-radius:10px; padding:10px; margin-top:8px; white-space:pre-wrap; word-break:break-word; }
  .preview { height:90px; border-radius:12px; border:1px solid rgba(255,255,255,.15); margin-top:8px; }
  .big-timer { font-size:44px; font-weight:700; text-align:center; letter-spacing:2px; margin:6px 0 12px; }
  .toast { position:fixed; top:70px; left:50%; transform:translateX(-50%) translateY(-20px); background:rgba(20,20,20,.95); border:1px solid rgba(255,255,255,.2); padding:8px 16px; border-radius:30px; font-size:12px; color:#fff; box-shadow:0 10px 30px rgba(0,0,0,.5); opacity:0; pointer-events:none; transition:.3s; z-index:300; max-width:90vw; text-align:center; }
  .toast.show { opacity:1; transform:translateX(-50%) translateY(0); }
  .btn-glass.active { border-color:var(--cyan); background:rgba(103,232,249,.15); color:var(--cyan); }
  .chat-container { display:flex; flex-direction:column; height:380px; background:rgba(0,0,0,.4); border:1px solid rgba(255,255,255,.1); border-radius:14px; padding:14px; overflow-y:auto; gap:12px; margin-bottom:12px; }
  .chat-msg { max-width:85%; padding:10px 14px; border-radius:14px; font-size:12.5px; line-height:1.5; white-space:pre-wrap; word-break:break-word; }
  .chat-msg.user { align-self:flex-end; background:linear-gradient(135deg,rgba(120,80,255,.4),rgba(0,200,255,.3)); border:1px solid rgba(255,255,255,.2); border-bottom-right-radius:2px; }
  .chat-msg.ai { align-self:flex-start; background:rgba(255,255,255,.07); border:1px solid rgba(255,255,255,.12); border-bottom-left-radius:2px; }
  .quick-prompts { display:flex; gap:6px; flex-wrap:wrap; }
  .loader { border:2px solid rgba(255,255,255,.25); border-top-color:#fff; border-radius:50%; width:14px; height:14px; animation:spin .8s linear infinite; display:inline-block; }
  @keyframes spin { to { transform:rotate(360deg); } }
</style>
</head>
<body>
<div class="bg-layer"></div>
<div class="toast" id="toast"></div>

<div id="authView" class="auth-wrap">
  <div class="card">
    <div id="loginBox">
      <div class="head">
        <div class="head-logo">🔐</div>
        <h1>NAMEKTUKAM Dashboard</h1>
        <p>Masuk untuk akses project tracker, preset library, dan tools.</p>
      </div>
      <form id="loginForm">
        <div class="field"><label>Username</label><input id="loginUser" autocomplete="username" maxlength="32" required /></div>
        <div class="field"><label>Password</label><input id="loginPass" type="password" autocomplete="current-password" required /></div>
        <button class="btn btn-primary" id="loginBtn">Masuk &rarr;</button>
      </form>
      <div class="switch-auth">Belum punya akun? <a onclick="showAuth('reg')">Daftar</a></div>
    </div>
    <div id="regBox" class="hidden">
      <div class="head">
        <div class="head-logo">✨</div>
        <h1>Buat Akun Baru</h1>
        <p>Cukup username dan password. Data tersimpan di browser ini.</p>
      </div>
      <form id="regForm">
        <div class="field"><label>Username (3–32: huruf, angka, _ - .)</label><input id="regUser" autocomplete="username" maxlength="32" required /></div>
        <div class="field"><label>Password (min. 8 karakter)</label><input id="regPass" type="password" autocomplete="new-password" required /></div>
        <div class="field"><label>Ulangi Password</label><input id="regPass2" type="password" autocomplete="new-password" required /></div>
        <button class="btn btn-primary">Daftar &rarr;</button>
      </form>
      <div class="switch-auth">Sudah punya akun? <a onclick="showAuth('login')">Masuk</a></div>
    </div>
  </div>
</div>

<div id="appView" class="hidden">
  <nav>
    <div class="brand"><div class="brand-logo">&lt;/&gt;</div><span>NAMEKTUKAM<b>Dev</b></span></div>
    <div class="nav-right">
      <div class="nav-tabs" id="navTabs">
        <div class="liquid-pill" id="pill"></div>
        <button class="tab-btn active" data-tab="home">🏠 Home</button>
        <button class="tab-btn" data-tab="am">🎬 AM Hub</button>
        <button class="tab-btn" data-tab="downloader">📥 Downloader</button>
        <button class="tab-btn" data-tab="tools">🧰 Tools</button>
        <button class="tab-btn" data-tab="ytta">▶️ YTTA</button>
        <button class="tab-btn" data-tab="ai">🤖 AI Hub</button>
        <button class="tab-btn" data-tab="admin">🛡️ Admin</button>
        <button class="tab-btn" data-tab="account">👤 Akun</button>
      </div>
      <span class="user-chip" id="userChip"></span>
    </div>
  </nav>

  <main>
    <div id="broadcastBanner" class="banner hidden"><span id="broadcastText"></span><button class="btn btn-glass btn-sm" onclick="dismissBanner()">Tutup</button></div>

    <section id="tab-home" class="tab-content active">
      <div class="card card-wide">
        <div class="head" style="text-align:left"><h1 id="homeGreet">Halo!</h1><p>Ringkasan aktivitas kamu di dashboard.</p></div>
        <div class="stats-grid">
          <div class="stat-card"><div class="label">Project Aktif</div><div class="val" id="stProj">0</div></div>
          <div class="stat-card"><div class="label">Project Selesai</div><div class="val" id="stDone">0</div></div>
          <div class="stat-card"><div class="label">Preset Tersimpan</div><div class="val" id="stPre">0</div></div>
          <div class="stat-card"><div class="label">Pomodoro Selesai</div><div class="val" id="stPomo">0</div></div>
        </div>
        <div class="grid3">
          <div class="sec"><h3>🎬 AM Hub</h3><p class="muted">Kelola project edit dan simpan preset keyframe/efek.</p><button class="btn btn-glass" style="margin-top:10px" onclick="goTab('am')">Buka</button></div>
          <div class="sec"><h3>🧰 Tools</h3><p class="muted">Kalkulator frame, resolusi, gradient, warna, dan Pomodoro.</p><button class="btn btn-glass" style="margin-top:10px" onclick="goTab('tools')">Buka</button></div>
          <div class="sec"><h3>▶️ YTTA</h3><p class="muted">Thumbnail grabber, tag generator, dan chapter formatter.</p><button class="btn btn-glass" style="margin-top:10px" onclick="goTab('ytta')">Buka</button></div>
        </div>
      </div>
    </section>

    <section id="tab-am" class="tab-content">
      <div class="card card-wide">
        <div class="head" style="text-align:left">
          <h1>Alight Motion Hub</h1>
          <p>Aktivasi Alight Motion Premium, generate akun bulk, serta organisir project dan preset kamu.</p>
        </div>
        <div class="grid2">
          <div class="sec">
            <h3>📧 Aktivasi Alight Motion Premium</h3>
            <div id="msgBox" class="msg-box"></div>
            <div class="field">
              <label>1. Email Alight Motion</label>
              <div class="row">
                <input type="email" id="emailInput" placeholder="nama@email.com" />
                <button type="button" class="btn btn-primary" id="sendBtn" style="width:auto;white-space:nowrap">Kirim Link</button>
              </div>
            </div>
            <div class="field" style="margin-top:10px">
              <label>2. Magic Link (dari Inbox Email)</label>
              <input type="text" id="magicInput" placeholder="Tempel URL Magic Link di sini..." />
            </div>
            <button type="button" class="btn btn-primary" id="activateBtn" style="margin-top:12px">
              <span id="btnText"><i class="fas fa-shield-halved"></i> Verifikasi & Aktifkan</span>
            </button>
            <div class="row" style="margin-top:14px">
              <button type="button" class="btn btn-glass" onclick="openOfficial()">Situs Resmi &rarr;</button>
              <button type="button" class="btn btn-glass" onclick="openSubs()">Cek Langganan</button>
            </div>
          </div>
          <div class="sec">
            <h3>⚡ Bulk Account Generator</h3>
            <div id="bulkMsgBox" class="msg-box"></div>
            <div class="field">
              <label>Jumlah Akun</label>
              <input type="number" id="bulkCount" value="1" min="1" max="10" />
            </div>
            <button type="button" class="btn btn-primary" id="bulkBtn" onclick="doBulk()" style="margin-top:12px">
              <span id="bulkBtnText"><i class="fas fa-layer-group"></i> Generate Akun Sekarang</span>
            </button>
            <div id="bulkResult" style="display:none; margin-top:14px">
              <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px">
                <b id="bulkResultLabel" style="font-size:12px; color:var(--cyan)"></b>
                <div class="row" style="width:auto; gap:6px">
                  <button type="button" class="btn btn-glass btn-sm" onclick="copyBulk()"><i class="fas fa-copy"></i> Salin</button>
                  <button type="button" class="btn btn-glass btn-sm" onclick="downloadBulk()"><i class="fas fa-download"></i> Unduh</button>
                </div>
              </div>
              <div id="bulkList"></div>
            </div>
          </div>
        </div>
        <div class="grid2" style="margin-top:16px">
          <div class="sec">
            <h3>➕ Project Baru</h3>
            <form id="projForm">
              <div class="field"><label>Judul project</label><input id="projTitle" maxlength="80" required /></div>
              <div class="row">
                <div class="field"><label>Deadline (opsional)</label><input type="date" id="projDue" /></div>
                <div class="field"><label>Status awal</label><select id="projStatus"><option value="idea">💡 Ide</option><option value="progress">🛠️ Dikerjakan</option><option value="done">✅ Selesai</option></select></div>
              </div>
              <button class="btn btn-primary">Tambah Project</button>
            </form>
          </div>
          <div class="sec">
            <h3>📋 Project Tracker</h3>
            <div class="kanban">
              <div class="col"><h4>💡 Ide</h4><div id="colIdea"></div></div>
              <div class="col"><h4>🛠️ Dikerjakan</h4><div id="colProgress"></div></div>
              <div class="col"><h4>✅ Selesai</h4><div id="colDone"></div></div>
            </div>
          </div>
        </div>
        <div class="sec">
          <h3>🎛️ Preset Library</h3>
          <form id="presetForm">
            <div class="row">
              <div class="field"><label>Nama preset</label><input id="preName" maxlength="60" required /></div>
              <div class="field"><label>Kategori</label><select id="preCat"><option>Keyframe</option><option>Efek</option><option>Teks</option><option>Transisi</option><option>Warna</option><option>Lainnya</option></select></div>
            </div>
            <div class="field"><label>Isi / catatan langkah / nilai parameter</label><textarea id="preBody" maxlength="2000" required></textarea></div>
            <button class="btn btn-primary">Simpan Preset</button>
          </form>
          <div class="row" style="margin-top:14px"><input id="preSearch" placeholder="Cari preset..." maxlength="100" /><select id="preFilter"><option value="">Semua kategori</option><option>Keyframe</option><option>Efek</option><option>Teks</option><option>Transisi</option><option>Warna</option><option>Lainnya</option></select></div>
          <div id="presetList" style="margin-top:12px"></div>
        </div>
      </div>
    </section>

    <!-- DOWNLOADER ASLI -->
    <section id="tab-downloader" class="tab-content">
      <div class="card card-wide">
        <div class="head" style="text-align:left">
          <h1>Download Video Lewat Link</h1>
          <p>Unduh video atau audio dari YouTube, TikTok, Instagram, Twitter, dll.</p>
        </div>
        <div class="sec">
          <form id="dlForm">
            <div class="field"><label>Link Video</label><input id="dlUrl" placeholder="https://www.youtube.com/watch?v=..." required /></div>
            <div class="field"><label>Format</label><select id="dlFormat"><option value="mp4">MP4 (Video)</option><option value="mp3">MP3 (Audio)</option></select></div>
            <button class="btn btn-primary" id="dlBtn">Mulai Unduh</button>
          </form>
          <div id="dlStatus" class="hidden" style="margin-top:16px">
            <p id="dlStatusText" class="muted">Menghubungkan...</p>
            <div style="background:rgba(255,255,255,.1);border-radius:10px;overflow:hidden;height:10px;margin-top:8px">
              <div id="dlBar" style="background:var(--cyan);width:0%;height:100%;transition:width .3s"></div>
            </div>
          </div>
          <div id="dlResult" class="hidden" style="margin-top:16px"></div>
        </div>
      </div>
    </section>

    <section id="tab-tools" class="tab-content">
      <div class="card card-wide">
        <div class="head" style="text-align:left"><h1>Tools Editor</h1><p>Semua dihitung langsung di browser.</p></div>
        <div class="grid2">
          <div class="sec">
            <h3>🎞️ Kalkulator Frame</h3>
            <div class="row">
              <div class="field"><label>FPS</label><select id="fcFps"><option>24</option><option>25</option><option>30</option><option selected>60</option><option>120</option></select></div>
              <div class="field"><label>Durasi (detik)</label><input type="number" id="fcSec" min="0" step="0.01" value="10" /></div>
              <div class="field"><label>atau Frame</label><input type="number" id="fcFrames" min="0" step="1" /></div>
            </div>
            <div class="out" id="fcOut">—</div>
          </div>
          <div class="sec">
            <h3>📐 Resolusi &amp; Aspect Ratio</h3>
            <div class="row">
              <div class="field"><label>Lebar</label><input type="number" id="arW" value="1920" min="1" /></div>
              <div class="field"><label>Tinggi</label><input type="number" id="arH" value="1080" min="1" /></div>
              <div class="field"><label>Skala ke lebar</label><input type="number" id="arTo" value="1280" min="1" /></div>
            </div>
            <div class="out" id="arOut">—</div>
          </div>
          <div class="sec">
            <h3>🌈 Gradient Generator</h3>
            <div class="row">
              <div class="field"><label>Warna 1</label><input type="color" id="gdA" value="#7850ff" /></div>
              <div class="field"><label>Warna 2</label><input type="color" id="gdB" value="#00c8ff" /></div>
              <div class="field"><label>Sudut (°)</label><input type="number" id="gdAng" value="135" min="0" max="360" /></div>
            </div>
            <div class="preview" id="gdPrev"></div>
            <div class="out" id="gdOut"></div>
            <button class="btn btn-glass" style="margin-top:8px" onclick="copyText(el('gdOut').textContent)">Salin CSS</button>
          </div>
          <div class="sec">
            <h3>🎨 Konverter Warna</h3>
            <div class="row">
              <div class="field"><label>Picker</label><input type="color" id="clPick" value="#ff3cb4" /></div>
              <div class="field"><label>HEX</label><input id="clHex" value="#ff3cb4" maxlength="7" /></div>
            </div>
            <div class="preview" id="clPrev" style="height:50px"></div>
            <div class="out" id="clOut"></div>
          </div>
          <div class="sec" style="grid-column:1/-1">
            <h3>🍅 Pomodoro Timer</h3>
            <div class="big-timer mono" id="pomoTime">25:00</div>
            <div class="row">
              <select id="pomoMode"><option value="25">Fokus 25 menit</option><option value="50">Fokus 50 menit</option><option value="5">Istirahat 5 menit</option><option value="15">Istirahat 15 menit</option></select>
              <button class="btn btn-primary" id="pomoStart">Mulai</button>
              <button class="btn btn-glass" id="pomoReset">Reset</button>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section id="tab-ytta" class="tab-content">
      <div class="card card-wide">
        <div class="head" style="text-align:left"><h1>YTTA Tools</h1><p>Alat bantu konten YouTube.</p></div>
        <div class="grid2">
          <div class="sec">
            <h3>🖼️ Thumbnail Grabber</h3>
            <div class="field"><label>Link video / ID video</label><input id="ytUrl" placeholder="https://youtu.be/xxxxxxxxxxx" /></div>
            <div id="ytOut" style="margin-top:10px"></div>
          </div>
          <div class="sec">
            <h3>🏷️ Tag Generator</h3>
            <div class="field"><label>Kata kunci (pisah koma / baris baru)</label><textarea id="tagIn" placeholder="alight motion, tutorial, edit video" maxlength="1000"></textarea></div>
            <div class="out" id="tagOut">—</div>
            <button class="btn btn-glass" style="margin-top:8px" onclick="copyText(el('tagOut').dataset.v||'')">Salin Tag</button>
          </div>
          <div class="sec">
            <h3>📝 Penghitung Judul</h3>
            <div class="field"><label>Judul video</label><input id="ttlIn" maxlength="150" /></div>
            <div class="out" id="ttlOut">0 / 100 karakter</div>
          </div>
          <div class="sec">
            <h3>⏱️ Chapter Formatter</h3>
            <div class="field"><label>Satu baris per chapter: "0:00 Intro"</label><textarea id="chIn" placeholder="0:00 Intro&#10;1:30 Materi&#10;5:10 Penutup" maxlength="2000"></textarea></div>
            <div class="out" id="chOut">—</div>
            <button class="btn btn-glass" style="margin-top:8px" onclick="copyText(el('chOut').dataset.v||'')">Salin Chapter</button>
          </div>
        </div>
      </div>
    </section>

    <section id="tab-ai" class="tab-content">
      <div class="card card-wide">
        <div class="head" style="text-align:left"><h1>🤖 AI Hub</h1><p>Asisten ide konten, skrip, dan SEO memakai Google Gemini API v1beta Terbaru.</p></div>
        <div class="grid2">
          <div class="sec">
            <h3>⚙️ API Key & Provider</h3>
            <p class="muted" style="margin-bottom:10px">Key disimpan lokal di browser. Mendukung berbagai model AI modern.</p>
            <div class="field"><label>Gemini API Key</label><input type="password" id="aiApiKey" placeholder="AIza..." autocomplete="off" /></div>
            <div class="field" style="margin-top:8px">
              <label>Pilihan Model AI (Terbaru)</label>
              <select id="aiModel">
                <option value="gemini-2.5-flash" selected>Gemini 2.5 Flash (Tercepat & Cerdas)</option>
                <option value="gemini-2.5-pro">Gemini 2.5 Pro (Paling Cerdas / Kompleks)</option>
                <option value="gemini-2.0-flash">Gemini 2.0 Flash</option>
                <option value="gemini-1.5-flash">Gemini 1.5 Flash</option>
                <option value="gemini-1.5-pro">Gemini 1.5 Pro</option>
              </select>
            </div>
            <div class="row" style="margin-top:10px"><button class="btn btn-primary" onclick="saveAiKey()">Simpan Setting AI</button><a class="btn btn-glass" href="https://aistudio.google.com/app/apikey" target="_blank" rel="noopener noreferrer">Dapatkan Key</a></div>
            <div id="aiKeyStatus" style="margin-top:10px"></div>
          </div>
          <div class="sec">
            <h3>⚡ Prompt Cepat</h3>
            <div class="quick-prompts">
              <button class="btn btn-glass btn-sm" data-p="Berikan 5 ide konten Alight Motion yang cocok untuk TikTok dan Shorts." onclick="quickPrompt(this.dataset.p)">💡 Ide Konten</button>
              <button class="btn btn-glass btn-sm" data-p="Buatkan skrip video pendek 30 detik tentang tutorial transisi smooth di Alight Motion." onclick="quickPrompt(this.dataset.p)">📝 Skrip Shorts</button>
              <button class="btn btn-glass btn-sm" data-p="Buatkan judul, deskripsi, dan tag YouTube SEO untuk video tutorial Alight Motion." onclick="quickPrompt(this.dataset.p)">🏷️ SEO YouTube</button>
              <button class="btn btn-glass btn-sm" data-p="Sarankan kombinasi efek dan keyframe di Alight Motion untuk membuat edit velocity yang aesthetic." onclick="quickPrompt(this.dataset.p)">🎨 Konsep Preset</button>
            </div>
          </div>
        </div>
        <div class="sec">
          <h3>💬 Chat</h3>
          <div class="chat-container" id="aiChatBox"><div class="chat-msg ai">Halo! Saya AI Assistant terupdate. Tanya apa saja soal ide konten, skrip, atau editing.</div></div>
          <div class="row"><input id="aiInput" placeholder="Ketik pertanyaan..." style="flex:4" maxlength="1000" /><button class="btn btn-primary" id="aiSendBtn" onclick="sendAiMessage()" style="flex:1">Kirim</button></div>
        </div>
      </div>
    </section>

    <section id="tab-admin" class="tab-content">
      <div class="card card-wide">
        <div id="adminLock" style="max-width:400px;margin:20px auto;text-align:center">
          <div class="head-logo">🛡️</div>
          <h1 style="font-size:18px;margin-bottom:6px" id="adminLockTitle">Akses Owner</h1>
          <p class="muted" id="adminLockDesc" style="margin-bottom:14px"></p>
          <form id="adminPinForm">
            <input type="password" id="adminPin" placeholder="PIN owner" style="text-align:center" required />
            <input type="password" id="adminPin2" placeholder="Ulangi PIN" style="text-align:center" class="hidden" />
            <button class="btn btn-primary" id="adminPinBtn">Buka Panel &rarr;</button>
          </form>
        </div>
        <div id="adminPanel" class="hidden">
          <div class="head" style="text-align:left;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid rgba(255,255,255,.1);padding-bottom:12px">
            <div><h1>Owner Control Center</h1><p style="color:var(--cyan)">NAMEKTUKAM Admin v4 (Real-Time Live Monitor Enabled)</p></div>
            <button class="btn btn-danger btn-sm" onclick="lockAdmin()">Kunci</button>
          </div>
          <div class="stats-grid">
            <div class="stat-card"><div class="label">Total User</div><div class="val" id="adUsers">0</div></div>
            <div class="stat-card"><div class="label">Total Log</div><div class="val" id="adLogs">0</div></div>
            <div class="stat-card"><div class="label">Server</div><div class="val" id="adServer">ONLINE</div></div>
            <div class="stat-card"><div class="label">Batas Item/User</div><div class="val" id="adQuota">200</div></div>
          </div>
          <div class="grid2">
            <div>
              <div class="sec">
                <h3>⚡ Quick Action</h3>
                <div style="display:flex;flex-direction:column;gap:8px">
                  <button class="btn btn-glass" id="btnMaint" onclick="toggleMaint()">Maintenance</button>
                  <button class="btn btn-danger" onclick="clearLogs()">Hapus Semua Log</button>
                  <button class="btn btn-glass" onclick="changeOwnerPin()">Ganti PIN Owner</button>
                </div>
              </div>
              <div class="sec">
                <h3>📊 Batas Item per User</h3>
                <form id="quotaForm"><input type="number" id="quotaIn" placeholder="mis. 200" min="1" max="5000" required /><button class="btn btn-primary">Update</button></form>
                <p class="muted" style="margin-top:8px">Membatasi jumlah project dan preset per akun.</p>
              </div>
            </div>
            <div>
              <div class="sec">
                <h3>🚫 Ban / Unban</h3>
                <form id="banForm"><input id="banIn" placeholder="Username" required /><div class="row"><button class="btn btn-danger">Ban</button><button type="button" class="btn btn-glass" onclick="unbanUser()">Unban</button></div></form>
              </div>
              <div class="sec">
                <h3>📢 Broadcast</h3>
                <form id="bcForm"><input id="bcIn" placeholder="Pesan pengumuman..." maxlength="200" /><div class="row"><button class="btn btn-primary">Kirim</button><button type="button" class="btn btn-glass" onclick="clearBroadcast()">Hapus</button></div></form>
              </div>
            </div>
          </div>
          <div class="sec">
            <h3>👥 Daftar User</h3>
            <div class="table-wrap"><table><thead><tr><th>Username</th><th>Email AM</th><th>Dibuat</th><th>Item</th><th>Status</th><th>Aksi</th></tr></thead><tbody id="userTable"></tbody></table></div>
          </div>
          <div class="sec">
            <h3>👁 Log Aktivitas</h3>
            <div class="row"><input id="logSearch" placeholder="Filter username / status..." /><button class="btn btn-glass" onclick="exportLogsCSV()">Export CSV</button></div>
            <div class="table-wrap"><table><thead><tr><th>Username</th><th>Email</th><th>Waktu</th><th>Aktivitas</th></tr></thead><tbody id="logTable"></tbody></table></div>
          </div>
          <div class="sec">
            <h3>💾 Backup &amp; Restore</h3>
            <div class="row"><button class="btn btn-glass" onclick="backupAll()">Download Backup (.json)</button><button class="btn btn-glass" onclick="el('restoreFile').click()">Restore dari File</button></div>
            <input type="file" id="restoreFile" accept="application/json" class="hidden" />
          </div>
        </div>
      </div>
    </section>

    <section id="tab-account" class="tab-content">
      <div class="card">
        <div class="head"><div class="head-logo">👤</div><h1 id="accName">Akun</h1><p id="accMeta"></p></div>
        <div class="sec">
          <h3>🔑 Ganti Password</h3>
          <form id="pwForm">
            <input type="password" id="pwOld" placeholder="Password lama" autocomplete="current-password" required />
            <input type="password" id="pwNew" placeholder="Password baru (min. 8)" autocomplete="new-password" required />
            <button class="btn btn-primary">Update Password</button>
          </form>
        </div>
        <div class="sec">
          <h3>📦 Data Saya</h3>
          <button class="btn btn-glass" onclick="exportMyData()">Download Data Saya (.json)</button>
        </div>
        <div class="sec">
          <h3>⚠️ Zona Berbahaya</h3>
          <div class="row"><button class="btn btn-glass" onclick="logout()">Keluar</button><button class="btn btn-danger" onclick="deleteMyAccount()">Hapus Akun</button></div>
        </div>
      </div>
    </section>
  </main>
</div>

<script>
const el = id => document.getElementById(id);
const esc = v => String(v ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"}[c]));
function safeUrl(url) { if (!url) return "#"; const str = String(url).trim(); return /^(https?:\/\/|\/)/i.test(str) ? esc(str) : "#"; }
const uid = () => (crypto.randomUUID ? crypto.randomUUID() : Date.now().toString(36) + Math.random().toString(36).slice(2));
const now = () => new Date().toLocaleString('id-ID', { dateStyle: 'short', timeStyle: 'short' });

const KEY = "namektukam_dashboard_v4";
const DEF = { users: [], bannedUsers: [], logs: [], quota: 200, maintenance: false, broadcast: "", broadcastId: 0, ownerPin: null, aiApiKey: "", aiModel: "gemini-2.5-flash" };
let state = load();
let me = "";
let adminOpen = false;

const rtChannel = new BroadcastChannel("namektukam_realtime_sync");
rtChannel.onmessage = (event) => {
  if (event.data && event.data.type === "STATE_UPDATE") {
    state = load();
    if (me && !curUser()) logout(true);
    if (me && isBanned(me)) { toast("Akun kamu di-BAN oleh Owner."); logout(true); }
    if (me) renderAll();
    if (adminOpen) renderAdmin();
  }
};

setInterval(() => {
  const currentLocal = localStorage.getItem(KEY);
  if (currentLocal) {
    state = load();
    if (adminOpen) renderAdmin();
  }
}, 1000);

function notifyStateChange() {
  save();
  rtChannel.postMessage({ type: "STATE_UPDATE", timestamp: Date.now() });
}

let loginAttempts = 0, loginLockUntil = 0, pinAttempts = 0, pinLockUntil = 0;

function load() {
  try {
    const s = JSON.parse(localStorage.getItem(KEY) || "null");
    if (s) return { ...DEF, ...s };
  } catch {}
  return { ...DEF };
}
function save() { try { localStorage.setItem(KEY, JSON.stringify(state)); } catch {} }

async function sha(s) {
  if (window.crypto && crypto.subtle) {
    const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
    return [...new Uint8Array(b)].map(x => x.toString(16).padStart(2, '0')).join('');
  }
  let h = 5381; for (const c of s) h = ((h << 5) + h + c.charCodeAt(0)) | 0; return 'weak' + h;
}
const newSalt = () => uid().replace(/-/g, '');
const findUser = n => state.users.find(u => u.username.toLowerCase() === String(n).toLowerCase());
const isBanned = n => state.bannedUsers.some(b => b.toLowerCase() === String(n).toLowerCase());
const curUser = () => findUser(me);

function log(status, email) {
  state.logs.unshift({ username: me || "-", email: email || curUser()?.amEmail || "-", status, time: now() });
  state.logs = state.logs.slice(0, 300);
  notifyStateChange();
}

function toast(msg) {
  const t = el('toast'); t.textContent = msg; t.classList.add('show');
  clearTimeout(toast.t); toast.t = setTimeout(() => t.classList.remove('show'), 3200);
}

async function copyText(v) {
  if (!v) return toast("Tidak ada yang disalin.");
  try { await navigator.clipboard.writeText(v); toast("Disalin ✓"); } catch { toast("Gagal menyalin."); }
}

function download(name, text, type) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([text], { type: type || 'application/json' }));
  a.download = name; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}

function guard() {
  if (!me) return false;
  if (isBanned(me)) { toast("Akun kamu di-BAN oleh Owner."); logout(true); return false; }
  if (state.maintenance && !adminOpen) { toast("Server maintenance."); return false; }
  return true;
}

function showAuth(w) { el('loginBox').classList.toggle('hidden', w !== 'login'); el('regBox').classList.toggle('hidden', w !== 'reg'); }

el('regForm').addEventListener('submit', async e => {
  e.preventDefault();
  const u = el('regUser').value.trim(), p = el('regPass').value, p2 = el('regPass2').value;
  if (!/^[a-zA-Z0-9_.-]{3,32}$/.test(u)) return toast("Username 3-32 karakter.");
  if (p.length < 8) return toast("Password min. 8 karakter.");
  if (p !== p2) return toast("Konfirmasi password beda.");
  if (findUser(u)) return toast("Username sudah dipakai.");
  const salt = newSalt();
  state.users.push({ username: u, salt, hash: await sha(salt + p), createdAt: Date.now(), amEmail: "", projects: [], presets: [], pomos: 0 });
  me = u; log("Registrasi"); notifyStateChange(); enterApp(); toast(\`Selamat datang, \${u}!\`);
});

el('loginForm').addEventListener('submit', async e => {
  e.preventDefault();
  if (Date.now() < loginLockUntil) return toast("Terkunci sementara.");
  const u = el('loginUser').value.trim(), p = el('loginPass').value;
  const user = findUser(u);
  if (isBanned(u)) return toast("Akun di-BAN.");
  let ok = user && user.hash === await sha(user.salt + p);
  if (!ok) {
    loginAttempts++;
    if (loginAttempts >= 5) { loginLockUntil = Date.now() + 30000; loginAttempts = 0; return toast("Terkunci 30 detik."); }
    return toast("Login salah.");
  }
  loginAttempts = 0;
  user.projects ||= []; user.presets ||= []; user.pomos ||= 0;
  me = user.username; log("Login"); notifyStateChange(); enterApp();
});

function enterApp() {
  el('authView').classList.add('hidden'); el('appView').classList.remove('hidden');
  el('loginForm').reset(); el('regForm').reset();
  renderAll(); goTab('home');
  setTimeout(() => movePill(document.querySelector('.tab-btn.active')), 50);
}

function logout(silent) {
  if (me && !silent) log("Logout");
  me = ""; adminOpen = false;
  el('appView').classList.add('hidden'); el('authView').classList.remove('hidden');
  lockAdmin(true); showAuth('login');
}

function movePill(btn) { const p = el('pill'); if (!btn) return; p.style.width = btn.offsetWidth + 'px'; p.style.left = btn.offsetLeft + 'px'; }
function goTab(id) {
  document.querySelectorAll('.tab-content').forEach(s => s.classList.toggle('active', s.id === 'tab-' + id));
  document.querySelectorAll('.tab-btn').forEach(b => { const a = b.dataset.tab === id; b.classList.toggle('active', a); if (a) { movePill(b); b.scrollIntoView({ inline: 'center', block: 'nearest', behavior: 'smooth' }); } });
  if (id === 'admin') renderAdminLock();
  if (id === 'home' || id === 'account') renderHome();
}
document.querySelectorAll('.tab-btn').forEach(b => b.addEventListener('click', () => goTab(b.dataset.tab)));

function renderAll() {
  const u = curUser(); if (!u) return;
  el('userChip').textContent = '@' + u.username;
  if(el('emailInput')) el('emailInput').value = u.amEmail || "";
  renderHome(); renderProjects(); renderPresets(); renderBanner(); renderAdmin(); updateAiKeyUI();
}

function renderHome() {
  const u = curUser(); if (!u) return;
  el('homeGreet').textContent = \`Halo, \${u.username}!\`;
  el('stProj').textContent = u.projects.filter(p => p.status !== 'done').length;
  el('stDone').textContent = u.projects.filter(p => p.status === 'done').length;
  el('stPre').textContent = u.presets.length;
  el('stPomo').textContent = u.pomos || 0;
  el('accName').textContent = '@' + u.username;
  el('accMeta').textContent = 'Terdaftar ' + new Date(u.createdAt).toLocaleDateString('id-ID');
}

function renderBanner() {
  const seen = sessionStorage.getItem('bcSeen');
  const show = state.broadcast && String(state.broadcastId) !== seen;
  el('broadcastBanner').classList.toggle('hidden', !show);
  el('broadcastText').textContent = '📢 ' + state.broadcast;
}
function dismissBanner() { try { sessionStorage.setItem('bcSeen', String(state.broadcastId)); } catch {} renderBanner(); }

const openOfficial = () => window.open("https://alightmotion.com/", "_blank", "noopener,noreferrer");
const openSubs = () => window.open("https://play.google.com/store/account/subscriptions", "_blank", "noopener,noreferrer");

const itemCount = u => u.projects.length + u.presets.length;
function overQuota(u) { if (itemCount(u) >= state.quota) { toast(\`Batas \${state.quota} item tercapai.\`); return true; } return false; }

el('projForm').addEventListener('submit', e => {
  e.preventDefault(); if (!guard()) return;
  const u = curUser(); if (overQuota(u)) return;
  u.projects.unshift({ id: uid(), title: el('projTitle').value.trim(), due: el('projDue').value, status: el('projStatus').value, created: Date.now() });
  e.target.reset(); log("Project dibuat"); notifyStateChange(); renderProjects(); renderHome(); toast("Project ditambahkan.");
});

const ORDER = ['idea', 'progress', 'done'];
function renderProjects() {
  const u = curUser(); if (!u) return;
  const map = { idea: 'colIdea', progress: 'colProgress', done: 'colDone' };
  ORDER.forEach(s => {
    const items = u.projects.filter(p => p.status === s);
    el(map[s]).innerHTML = items.length ? items.map(p => \`<div class="item"><b>\${esc(p.title)}</b><div class="acts"><button class="btn btn-glass btn-sm" onclick="delProj('\${p.id}')">✕</button></div></div>\`).join('') : '<p class="muted">Kosong.</p>';
  });
}
function delProj(id) {
  if (!guard()) return; const u = curUser(); u.projects = u.projects.filter(p => p.id !== id);
  notifyStateChange(); renderProjects(); renderHome();
}

el('presetForm').addEventListener('submit', e => {
  e.preventDefault(); if (!guard()) return;
  const u = curUser(); if (overQuota(u)) return;
  u.presets.unshift({ id: uid(), name: el('preName').value.trim(), cat: el('preCat').value, body: el('preBody').value, created: Date.now() });
  e.target.reset(); log("Preset disimpan"); notifyStateChange(); renderPresets(); renderHome(); toast("Preset disimpan.");
});

function renderPresets() {
  const u = curUser(); if (!u) return;
  el('presetList').innerHTML = u.presets.length ? u.presets.map(p => \`<div class="preset"><b>\${esc(p.name)}</b><pre>\${esc(p.body)}</pre></div>\`).join('') : '<p class="muted">Belum ada.</p>';
}

// SCRIPT DOWNLOADER ASLI (yt-dlp backend integration)
el('dlForm').addEventListener('submit', async e => {
  e.preventDefault();
  const url = el('dlUrl').value.trim();
  const format = el('dlFormat').value;
  if (!url) return toast('Masukkan link video.');

  el('dlBtn').disabled = true;
  el('dlStatus').classList.remove('hidden');
  el('dlResult').classList.add('hidden');
  el('dlBar').style.width = '0%';
  el('dlStatusText').textContent = 'Memulai unduhan...';

  try {
    const res = await fetch('/api/dl/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url, format })
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'Gagal memulai unduhan.');

    const eventSource = new EventSource(`/api/dl/progress/${data.id}`);
    eventSource.onmessage = event => {
      const job = JSON.parse(event.data);
      el('dlBar').style.width = job.percent + '%';
      el('dlStatusText').textContent = `${job.title} — ${job.text} (${job.percent}%)`;

      if (job.status === 'done') {
        eventSource.close();
        el('dlBtn').disabled = false;
        el('dlStatus').classList.add('hidden');
        el('dlResult').classList.remove('hidden');
        el('dlResult').innerHTML = \`<div class="sec"><h3>Unduhan Selesai!</h3><p class="muted">\${esc(job.title)}</p><a class="btn btn-primary" href="/api/dl/file/\${data.id}" style="margin-top:10px" download>Unduh File Sekarang &rarr;</a></div>\`;
      } else if (job.status === 'error') {
        eventSource.close();
        el('dlBtn').disabled = false;
        el('dlStatus').classList.add('hidden');
        toast(job.error || 'Terjadi kesalahan.');
      }
    };
    eventSource.onerror = () => {
      eventSource.close();
      el('dlBtn').disabled = false;
      el('dlStatus').classList.add('hidden');
      toast('Koneksi terputus.');
    };
  } catch (err) {
    el('dlBtn').disabled = false;
    el('dlStatus').classList.add('hidden');
    toast(err.message);
  }
});

// TOOLS
function calcFrames() {
  const fps = +el('fcFps').value, sec = +el('fcSec').value || 0, fr = Math.round(sec * fps);
  el('fcFrames').value = fr;
  el('fcOut').textContent = \`\${sec} detik × \${fps} fps = \${fr} frame\`;
}
el('fcFps').addEventListener('change', calcFrames); el('fcSec').addEventListener('input', calcFrames);

// POMODORO
let pomo = { left: 1500, run: false, timer: null };
function drawPomo() { el('pomoTime').textContent = \`\${String(Math.floor(pomo.left / 60)).padStart(2, '0')}:\${String(pomo.left % 60).padStart(2, '0')}\`; }
el('pomoStart').addEventListener('click', () => {
  if (pomo.run) { clearInterval(pomo.timer); pomo.run = false; return; }
  pomo.run = true;
  pomo.timer = setInterval(() => { pomo.left--; drawPomo(); if(pomo.left <= 0) { clearInterval(pomo.timer); pomo.run = false; toast("Waktu habis!"); } }, 1000);
});
el('pomoReset').addEventListener('click', () => { clearInterval(pomo.timer); pomo.run = false; pomo.left = 1500; drawPomo(); });

// AI HUB
function updateAiKeyUI() {
  el('aiApiKey').value = state.aiApiKey || '';
  el('aiKeyStatus').innerHTML = state.aiApiKey ? '<span class="badge b-green">✓ Terpasang</span>' : '<span class="badge b-red">Belum diatur</span>';
}
function saveAiKey() {
  state.aiApiKey = el('aiApiKey').value.trim();
  notifyStateChange(); updateAiKeyUI(); toast("Disimpan.");
}
async function sendAiMessage() {
  const text = el('aiInput').value.trim(), key = state.aiApiKey;
  if (!text || !key) return toast("Isi pesan & API key.");
  const d = document.createElement('div'); d.className = 'chat-msg user'; d.textContent = text;
  el('aiChatBox').appendChild(d); el('aiInput').value = '';
  try {
    const r = await fetch(\`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=\${key}\`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ contents: [{ parts: [{ text }] }] })
    });
    const res = await r.json();
    const reply = res.candidates?.[0]?.content?.parts?.[0]?.text || "Gagal merespons.";
    const ai = document.createElement('div'); ai.className = 'chat-msg ai'; ai.textContent = reply;
    el('aiChatBox').appendChild(ai);
  } catch { toast("Gagal terhubung ke AI."); }
}

// ADMIN & ACCOUNT
function renderAdminLock() {
  el('adminLock').classList.toggle('hidden', adminOpen);
  el('adminPanel').classList.toggle('hidden', !adminOpen);
  if(adminOpen) renderAdmin();
}
el('adminPinForm').addEventListener('submit', async e => {
  e.preventDefault();
  const p = el('adminPin').value;
  if (!state.ownerPin) {
    const salt = newSalt(); state.ownerPin = { salt, hash: await sha(salt + p) }; notifyStateChange();
  } else if (state.ownerPin.hash !== await sha(state.ownerPin.salt + p)) { return toast("PIN salah."); }
  adminOpen = true; renderAdminLock();
});
function lockAdmin(silent) { adminOpen = false; renderAdminLock(); }
function renderAdmin() {
  if(!adminOpen) return;
  el('adUsers').textContent = state.users.length;
  el('userTable').innerHTML = state.users.map(u => \`<tr><td><b>\${esc(u.username)}</b></td><td>\${esc(u.amEmail||'-')}</td><td>\${new Date(u.createdAt).toLocaleDateString()}</td><td>Aktif</td><td><button class="btn btn-danger btn-sm" onclick="delUsr('\${u.username}')">Hapus</button></td></tr>\`).join('');
}
function delUsr(n) { state.users = state.users.filter(u => u.username !== n); notifyStateChange(); renderAdmin(); }
function logout(silent) { me = ""; el('appView').classList.add('hidden'); el('authView').classList.remove('hidden'); showAuth('login'); }
function backupAll() { download('backup.json', JSON.stringify(state, null, 2)); }
</script>
</body>
</html>`;

app.get('/', (req, res) => {
  res.send(HTML_CONTENT);
});

app.listen(PORT, () => console.log(`🚀 Server NAMEKTUKAM berjalan di http://localhost:${PORT}`));
