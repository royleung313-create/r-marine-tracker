// ==========================================
// R! Marine - AISstream 實時座標接收器 (tracker.js)
// 自動讀取 Google Sheets 船隻名單並寫回 24h 軌跡
// ==========================================

const API_KEY = (process.env.AISSTREAM_API_KEY || '').trim();
const GAS_URL = (process.env.GAS_URL || '').trim();
const LISTEN_TIMEOUT_MS = 210 * 1000; // 最多監聽 3.5 分鐘

if (!API_KEY || !GAS_URL) {
  console.error('❌ 缺少 AISSTREAM_API_KEY 或 GAS_URL 環境變數');
  process.exit(1);
}

if (!GAS_URL.startsWith('https://')) {
  console.error('❌ GAS_URL 格式唔正確（必須以 https:// 開頭），請檢查有冇同 AISSTREAM_API_KEY 掉轉貼錯！');
  process.exit(1);
}

async function getTargetMmsis() {
  const res = await fetch(`${GAS_URL}?t=${Date.now()}`, { redirect: 'follow' });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  const list = (data.vessels || [])
    .map(v => String(v.mmsi || '').replace(/\D/g, '').trim())
    .filter(Boolean);
  if (list.length > 0) {
    return [...new Set(list)];
  }
  return ['477995357'];
}

async function postTrackPoint(payload) {
  const res = await fetch(GAS_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    redirect: 'follow'
  });
  const text = await res.text();
  console.log(`✅ 已寫入 ${payload.mmsi} (${payload.lat.toFixed(5)}, ${payload.lng.toFixed(5)} | ${payload.speed}kts) -> ${text}`);
}

async function decodeMessage(data) {
  if (typeof data === 'string') return data;
  if (data instanceof Blob) return await data.text();
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString('utf8');
  return String(data);
}

async function main() {
  let targets = ['477995357'];
  try {
    targets = await getTargetMmsis();
    console.log('✅ 成功從 Google Sheets 讀取最愛名單：', targets.join(', '));
  } catch (err) {
    console.warn('⚠️ 讀取 Google Sheets 名單失敗，改用預設 MMSI：', err.message);
  }

  const recorded = new Set();
  const seenOtherShips = new Set();
  const pendingPosts = [];

  await new Promise((resolve) => {
    let finished = false;
    const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');

    const finish = (reason) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      console.log('⏹️ 結束監聽：', reason);
      try { ws.close(); } catch (e) {}
      resolve();
    };

    const timer = setTimeout(() => {
      finish(`已達監聽上限（收到目標 ${recorded.size}/${targets.length} 隻船；期間香港周邊共收到 ${seenOtherShips.size} 隻其他船訊號）`);
    }, LISTEN_TIMEOUT_MS);

    ws.addEventListener('open', () => {
      console.log('📡 已連線 AISstream，開始監聽香港及周邊水域訊號...');
      ws.send(JSON.stringify({
        APIKey: API_KEY,
        // 涵蓋香港全境、南丫島以南、蒲台島、澳門及珠江口
        BoundingBoxes: [[[21.5, 113.0], [23.0, 115.2]]],
        FilterMessageTypes: [
          'PositionReport',
          'StandardClassBPositionReport',
          'ExtendedClassBPositionReport'
        ]
      }));
    });

    ws.addEventListener('message', async (event) => {
      try {
        const raw = await decodeMessage(event.data);
        const msg = JSON.parse(raw);

        if (msg.error) {
          console.error('❌ AISstream 回傳錯誤：', msg.error);
          finish('AISstream API 錯誤');
          return;
        }

        const meta = msg.MetaData || {};
        const type = msg.MessageType;
        const body = (msg.Message && msg.Message[type]) ? msg.Message[type] : {};
        const mmsi = String(meta.MMSI || body.UserID || '').trim();
        if (!mmsi) return;

        if (!targets.includes(mmsi)) {
          seenOtherShips.add(mmsi);
          return;
        }
        if (recorded.has(mmsi)) return;

        const lat = Number(body.Latitude ?? meta.latitude);
        const lng = Number(body.Longitude ?? meta.longitude);
        if (isNaN(lat) || isNaN(lng) || Math.abs(lat) > 90 || Math.abs(lng) > 180 || (lat === 0 && lng === 0)) {
          return;
        }

        const speed = Number(body.Sog ?? 0);
        let course = Number(body.Cog ?? 0);
        if (isNaN(course) || course >= 360) {
          const hdg = Number(body.TrueHeading ?? 0);
          course = (!isNaN(hdg) && hdg < 360) ? hdg : 0;
        }

        recorded.add(mmsi);
        const p = postTrackPoint({
          mmsi,
          lat,
          lng,
          speed: isNaN(speed) ? 0 : Math.round(speed * 10) / 10,
          course: Math.round(course),
          timestamp: new Date().toISOString(),
          source: 'AIS'
        }).catch(err => console.error(`❌ 寫入 ${mmsi} 失敗：`, err.message));

        pendingPosts.push(p);

        if (recorded.size >= targets.length) {
          finish(`已收齊所有目標船隻最新座標（期間香港周邊收到 ${seenOtherShips.size} 隻其他船）`);
        }
      } catch (err) {
        console.warn('⚠️ 解析訊號略過：', err.message);
      }
    });

    ws.addEventListener('error', (err) => {
      console.error('⚠️ WebSocket 連線異常：', err.message || err);
      finish('連線中斷');
    });

    ws.addEventListener('close', (ev) => {
      finish(`連線已關閉 (code: ${ev.code}${ev.reason ? ', reason: ' + ev.reason : ''})`);
    });
  });

  await Promise.allSettled(pendingPosts);
  console.log(`🏁 本輪任務完成，共寫入 ${recorded.size} 筆實時座標。`);
}

main().catch(err => {
  console.error('❌ 執行失敗：', err);
  process.exit(1);
});
