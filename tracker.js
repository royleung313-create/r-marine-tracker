// ==========================================
// R! Marine - GitHub Actions AIS 實時爬蟲 (tracker.js v2)
// 使用 Node.js 22 內建 WebSocket 與 fetch（零外部套件）
// 新增：自動同步追蹤名單、香港海域收船統計、爬蟲心跳回報至 Code.gs v7
// ==========================================

const API_KEY = (process.env.AISSTREAM_API_KEY || '').trim();
const GAS_URL = (process.env.GAS_URL || '').trim();
const DEFAULT_MMSI = '477995357'; // PACIFIC TRUST 保底目標

// 監聽香港及珠江口海域範圍（用來同時統計周邊收船數，驗證 AISstream 連線健康）
const HK_BOUNDING_BOX = [[[21.6, 113.3], [22.8, 114.8]]];
const LISTEN_SECONDS = 210; // 最長收聽 3 分半鐘
const MIN_EARLY_EXIT_SECONDS = 25; // 若全部目標船都捉齊，至少聽夠 25 秒再提早收工

async function fetchTargetMmsis() {
  const targets = new Set([DEFAULT_MMSI]);
  try {
    const sep = GAS_URL.includes('?') ? '&' : '?';
    const res = await fetch(`${GAS_URL}${sep}t=${Date.now()}`, {
      method: 'GET',
      redirect: 'follow'
    });
    if (res.ok) {
      const data = await res.json();
      if (data && Array.isArray(data.vessels)) {
        for (const v of data.vessels) {
          const mmsi = String(v.mmsi || '').replace(/\D/g, '').trim();
          if (mmsi) targets.add(mmsi);
        }
      }
    }
  } catch (err) {
    console.warn('讀取後台追蹤池失敗，先以預設船隻運行：', err.message);
  }
  return targets;
}

async function postToGas(payload) {
  const res = await fetch(GAS_URL, {
    method: 'POST',
    redirect: 'follow',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`GAS HTTP ${res.status}: ${text.slice(0, 120)}`);
  }
  return text;
}

async function decodeWsData(raw) {
  if (typeof raw === 'string') return raw;
  if (raw instanceof Blob) return await raw.text();
  if (raw instanceof ArrayBuffer) return Buffer.from(raw).toString('utf8');
  if (ArrayBuffer.isView(raw)) {
    return Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength).toString('utf8');
  }
  return String(raw || '');
}

function extractVesselReport(msg) {
  if (!msg || !msg.MetaData) return null;
  const meta = msg.MetaData;
  const mmsi = String(meta.MMSI || '').replace(/\D/g, '').trim();
  if (!mmsi) return null;

  const body =
    (msg.Message && (
      msg.Message.PositionReport ||
      msg.Message.StandardClassBPositionReport ||
      msg.Message.ExtendedClassBPositionReport
    )) || null;

  const lat = Number(body && body.Latitude !== undefined ? body.Latitude : meta.latitude);
  const lng = Number(body && body.Longitude !== undefined ? body.Longitude : meta.longitude);

  if (!Number.isFinite(lat) || !Number.isFinite(lng) || lat === 0 || lng === 0) {
    return null;
  }

  const rawSpeed = body && body.Sog !== undefined ? Number(body.Sog) : 0;
  const rawCourse = body && body.Cog !== undefined ? Number(body.Cog) : 0;

  return {
    mmsi,
    lat: Math.round(lat * 100000) / 100000,
    lng: Math.round(lng * 100000) / 100000,
    speed: Number.isFinite(rawSpeed) && rawSpeed < 102 ? Math.round(rawSpeed * 10) / 10 : 0,
    course: Number.isFinite(rawCourse) && rawCourse <= 360 ? Math.round(rawCourse) : 0,
    timestamp: new Date().toISOString(),
    source: 'AISstream'
  };
}

function listenAisStream(targetSet) {
  return new Promise((resolve) => {
    const startMs = Date.now();
    const heardMmsis = new Set();
    const matchedMap = new Map();

    let finished = false;
    let status = 'OK';
    let note = '正常完成';
    let ws = null;
    let hardTimer = null;

    const finish = (finalStatus, finalNote) => {
      if (finished) return;
      finished = true;
      if (finalStatus) status = finalStatus;
      if (finalNote) note = finalNote;
      if (hardTimer) clearTimeout(hardTimer);
      try {
        if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
          ws.close();
        }
      } catch (e) {}
      resolve({
        status,
        note,
        heardCount: heardMmsis.size,
        matchedList: Array.from(matchedMap.values()),
        durationSec: Math.max(1, Math.round((Date.now() - startMs) / 1000))
      });
    };

    hardTimer = setTimeout(() => {
      finish('OK', matchedMap.size > 0 ? '定時收聽完成' : '正常收聽完成（目標船暫無廣播）');
    }, LISTEN_SECONDS * 1000);

    try {
      ws = new WebSocket('wss://stream.aisstream.io/v0/stream');
    } catch (err) {
      finish('ERROR', '建立連線失敗：' + err.message);
      return;
    }

    ws.addEventListener('open', () => {
      console.log('已連線至 AISstream，開始監聽香港水域...');
      const subMsg = {
        APIKey: API_KEY,
        BoundingBoxes: HK_BOUNDING_BOX,
        FilterMessageTypes: [
          'PositionReport',
          'StandardClassBPositionReport',
          'ExtendedClassBPositionReport'
        ]
      };
      ws.send(JSON.stringify(subMsg));
    });

    ws.addEventListener('message', async (event) => {
      try {
        const text = await decodeWsData(event.data);
        const msg = JSON.parse(text);
        if (msg.error) {
          finish('ERROR', 'AISstream 錯誤：' + String(msg.error));
          return;
        }

        const report = extractVesselReport(msg);
        if (!report) return;

        heardMmsis.add(report.mmsi);

        if (targetSet.has(report.mmsi)) {
          matchedMap.set(report.mmsi, report);
          console.log(`🎯 命中目標船 ${report.mmsi}：(${report.lat}, ${report.lng}) ${report.speed} kts`);

          const elapsedSec = (Date.now() - startMs) / 1000;
          if (matchedMap.size >= targetSet.size && elapsedSec >= MIN_EARLY_EXIT_SECONDS) {
            finish('OK', '已捕捉全部目標船，提早完成');
          }
        }
      } catch (e) {
        // 忽略個別格式異常封包
      }
    });

    ws.addEventListener('error', () => {
      finish('ERROR', 'AISstream 連線異常中斷');
    });

    ws.addEventListener('close', (ev) => {
      if (!finished) {
        const reason = ev && ev.code ? `連線提前關閉 (code ${ev.code})` : '連線提前關閉';
        finish(heardMmsis.size > 0 ? 'OK' : 'WARN', reason);
      }
    });
  });
}

async function main() {
  if (!API_KEY || !GAS_URL) {
    console.error('❌ 缺少環境變數：請確認 GitHub Secrets 已設定 AISSTREAM_API_KEY 同 GAS_URL');
    process.exit(1);
  }

  console.log('1. 正在向 Google 後台讀取最新追蹤船隻名單...');
  const targetSet = await fetchTargetMmsis();
  console.log(`📋 本輪追蹤目標 (${targetSet.size} 隻)：${Array.from(targetSet).join(', ')}`);

  console.log(`2. 開始收聽香港水域 AIS 船訊（最長 ${LISTEN_SECONDS} 秒）...`);
  const result = await listenAisStream(targetSet);
  console.log(`📡 收聽結束：香港周邊共收到 ${result.heardCount} 隻船，命中目標 ${result.matchedList.length}/${targetSet.size} 隻（耗時 ${result.durationSec} 秒）`);

  // 寫入有命中嘅目標船座標
  let uploadedCount = 0;
  for (const pt of result.matchedList) {
    try {
      await postToGas(pt);
      uploadedCount++;
      console.log(`✅ 已寫入座標：MMSI ${pt.mmsi}`);
    } catch (err) {
      console.error(`⚠️ 寫入座標失敗 (${pt.mmsi})：`, err.message);
    }
  }

  // 無論有冇命中目標船，都發送爬蟲心跳俾 Code.gs 記錄，供站長暗門顯示
  try {
    await postToGas({
      action: 'heartbeat',
      status: result.status,
      totalHeard: result.heardCount,
      matchedCount: uploadedCount,
      targetCount: targetSet.size,
      durationSec: result.durationSec,
      note: result.note
    });
    console.log('💓 爬蟲心跳已成功回報至 Google 後台');
  } catch (err) {
    console.error('⚠️ 心跳回報失敗：', err.message);
  }
}

main().catch((err) => {
  console.error('程式執行錯誤：', err);
  process.exit(1);
});
