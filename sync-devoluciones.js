/**
 * sync-devoluciones.js
 * Devoluciones Amazon SP-API → Supabase  (workflow INDEPENDIENTE del de ventas)
 *
 * Trae 3 informes vía Reports API (flujo asíncrono: pedir → esperar → descargar):
 *   1. GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE   → devoluciones FBM
 *   2. GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA   → devoluciones FBA (con disposición)
 *   3. GET_FBA_REIMBURSEMENTS_DATA                 → indemnizaciones de Amazon
 *
 * Los informes admiten máximo 60 días por petición → se trocea en ventanas.
 *
 * Uso:
 *   node sync-devoluciones.js              → últimos 60 días (nocturno)
 *   node sync-devoluciones.js --desde=2026-01-01   → histórico completo
 */

const zlib = require('zlib');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SECRET = process.env.SUPABASE_SECRET;
const AMAZON_CLIENT_ID = process.env.AMAZON_CLIENT_ID;
const AMAZON_CLIENT_SECRET = process.env.AMAZON_CLIENT_SECRET;
const AMAZON_REFRESH_TOKEN = process.env.AMAZON_REFRESH_TOKEN;
const MARKETPLACE = 'A1RKKUPIHCS9HS';
const BASE = 'https://sellingpartnerapi-eu.amazon.com';

const crypto = require('crypto');
const hash = s => crypto.createHash('md5').update(String(s)).digest('hex').slice(0, 24);
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ============================================================
// SUPABASE
// ============================================================
async function supabase(method, table, data = null, query = '', prefer = 'resolution=merge-duplicates') {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}${query}`, {
    method,
    headers: {
      'apikey': SUPABASE_SECRET,
      'Authorization': `Bearer ${SUPABASE_SECRET}`,
      'Content-Type': 'application/json',
      'Prefer': method === 'POST' ? prefer : '',
    },
    body: data ? JSON.stringify(data) : undefined,
  });
  if (!res.ok) throw new Error(`Supabase ${method} ${table}: ${res.status} ${await res.text()}`);
  const text = await res.text();
  return text ? JSON.parse(text) : null;
}

async function upsertBatched(table, rows, size = 200) {
  for (let i = 0; i < rows.length; i += size) {
    await supabase('POST', table, rows.slice(i, i + size));
    console.log(`    guardados ${Math.min(i + size, rows.length)}/${rows.length}`);
  }
}

// ============================================================
// AMAZON
// ============================================================
async function getAccessToken() {
  const res = await fetch('https://api.amazon.com/auth/o2/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(AMAZON_REFRESH_TOKEN)}&client_id=${encodeURIComponent(AMAZON_CLIENT_ID)}&client_secret=${encodeURIComponent(AMAZON_CLIENT_SECRET)}`
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('Auth failed: ' + JSON.stringify(data));
  return data.access_token;
}

async function spApi(path, token, method = 'GET', body = null) {
  for (let attempt = 0; attempt < 6; attempt++) {
    const res = await fetch(`${BASE}${path}`, {
      method,
      headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (res.status === 429) {                       // rate limit → esperar y reintentar
      const wait = 2000 * (attempt + 1);
      console.log(`    rate limit, esperando ${wait / 1000}s...`);
      await sleep(wait);
      continue;
    }
    const text = await res.text();
    if (!res.ok) throw new Error(`SP-API ${res.status}: ${text.slice(0, 300)}`);
    return text ? JSON.parse(text) : {};
  }
  throw new Error('SP-API: demasiados reintentos por rate limit');
}

/**
 * Reports API: pide un informe, espera a que esté listo y lo descarga ya parseado.
 * Devuelve un array de objetos (una fila = un objeto con las cabeceras como claves).
 */
async function fetchReport(reportType, token, startISO, endISO) {
  console.log(`  → Pidiendo ${reportType}`);
  console.log(`     ventana: ${startISO.slice(0, 10)} → ${endISO.slice(0, 10)}`);

  // 1. Crear el informe
  const created = await spApi('/reports/2021-06-30/reports', token, 'POST', {
    reportType,
    marketplaceIds: [MARKETPLACE],
    dataStartTime: startISO,
    dataEndTime: endISO,
  });
  const reportId = created.reportId;
  if (!reportId) throw new Error('No reportId: ' + JSON.stringify(created));

  // 2. Esperar a que Amazon lo genere (puede tardar minutos)
  let doc = null;
  for (let i = 0; i < 60; i++) {                     // hasta ~10 min
    await sleep(10000);
    const st = await spApi(`/reports/2021-06-30/reports/${reportId}`, token);
    if (i % 3 === 0) console.log(`     estado: ${st.processingStatus}`);
    if (st.processingStatus === 'DONE') { doc = st.reportDocumentId; break; }
    if (st.processingStatus === 'CANCELLED') { console.log('     CANCELLED (sin datos en esta ventana)'); return []; }
    if (st.processingStatus === 'FATAL') { console.log('     FATAL: Amazon no pudo generarlo'); return []; }
  }
  if (!doc) { console.log('     timeout esperando el informe'); return []; }

  // 3. Descargar el documento
  const dl = await spApi(`/reports/2021-06-30/documents/${doc}`, token);
  const resp = await fetch(dl.url);
  const buf = Buffer.from(await resp.arrayBuffer());
  const content = dl.compressionAlgorithm === 'GZIP'
    ? zlib.gunzipSync(buf).toString('utf-8')
    : buf.toString('utf-8');

  // 4. Parsear TSV
  const rows = parseTsv(content);
  console.log(`     ✓ ${rows.length} filas`);
  return rows;
}

// Parseo de fichero delimitado por tabuladores
function parseTsv(text) {
  const lines = text.split(/\r?\n/).filter(l => l.trim() !== '');
  if (lines.length < 2) return [];
  const headers = lines[0].split('\t').map(h => h.trim());
  return lines.slice(1).map(line => {
    const cells = line.split('\t');
    const obj = {};
    headers.forEach((h, i) => { obj[h] = (cells[i] ?? '').trim(); });
    return obj;
  });
}

// ---- helpers de conversión ----
const num = v => { const n = parseFloat(String(v ?? '').replace(',', '.')); return isNaN(n) ? 0 : n; };
const int = v => { const n = parseInt(String(v ?? ''), 10); return isNaN(n) ? 0 : n; };
const dat = v => { if (!v) return null; const d = new Date(v); return isNaN(d) ? null : d.toISOString(); };
// busca una clave en la fila probando varios nombres (las cabeceras varían de idioma/versión)
const pick = (row, ...keys) => {
  for (const k of keys) if (row[k] !== undefined && row[k] !== '') return row[k];
  return '';
};

// ============================================================
// 1. DEVOLUCIONES FBM
// ============================================================
async function syncReturnsFbm(token, windows) {
  console.log('\n📦 DEVOLUCIONES FBM');
  let total = 0;
  for (const w of windows) {
    const rows = await fetchReport('GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE', token, w.start, w.end);
    if (!rows.length) continue;
    if (total === 0 && rows[0]) console.log('     columnas:', Object.keys(rows[0]).join(' | '));

    const toInsert = rows.map(r => {
      const orderId = pick(r, 'Order ID', 'order-id', 'Order-ID');
      const asin    = pick(r, 'ASIN', 'asin');
      const rma     = pick(r, 'Amazon RMA ID', 'Amazon-RMA-ID');
      return {
        id: hash(`${rma}|${orderId}|${asin}`),
        order_id: orderId,
        order_date:          dat(pick(r, 'Order date', 'Order-date')),
        return_request_date: dat(pick(r, 'Return request date', 'Return-request-date')),
        return_request_status: pick(r, 'Return request status', 'Return-request-status'),
        amazon_rma_id: rma,
        asin,
        sku:        pick(r, 'Merchant SKU', 'Merchant-SKU'),
        item_name:  pick(r, 'Item Name', 'Item-Name'),
        return_quantity: int(pick(r, 'Return quantity', 'Return-quantity')),
        return_reason:   pick(r, 'Return Reason', 'Return-Reason'),
        in_policy:       pick(r, 'In policy', 'In-policy'),
        return_type:     pick(r, 'Return type', 'Return-type'),
        resolution:      pick(r, 'Resolution'),
        label_type:      pick(r, 'Label type', 'Label-type'),
        label_cost:      num(pick(r, 'Label cost', 'Label-cost')),
        label_paid_by:   pick(r, 'Label to be paid by', 'Label-to-be-paid-by'),
        return_carrier:  pick(r, 'Return carrier', 'Return-carrier'),
        tracking_id:     pick(r, 'Tracking ID', 'Tracking-ID'),
        a_to_z_claim:    pick(r, 'A-to-Z Claim', 'A-to-Z-Claim'),
        is_prime:        pick(r, 'Is prime', 'Is-prime'),
        return_delivery_date: dat(pick(r, 'Return delivery date', 'Return-delivery-date')),
        order_amount:    num(pick(r, 'Order Amount', 'Order-Amount')),
        order_quantity:  int(pick(r, 'Order quantity', 'Order-quantity')),
        refunded_amount: num(pick(r, 'Refunded Amount', 'Refunded-Amount')),
        safet_claim_id:    pick(r, 'SafeT claim id', 'SafeT-claim-id'),
        safet_claim_state: pick(r, 'SafeT claim state', 'SafeT-claim-state'),
        safet_reimbursement: num(pick(r, 'SafeT claim reimbursement amount', 'SafeT-claim-reimbursement-amount')),
        raw: r,
        updated_at: new Date().toISOString(),
      };
    }).filter(x => x.order_id);

    await upsertBatched('returns_fbm', toInsert);
    total += toInsert.length;
    await sleep(2000);
  }
  console.log(`✓ FBM: ${total} devoluciones`);
  return total;
}

// ============================================================
// 2. DEVOLUCIONES FBA
// ============================================================
async function syncReturnsFba(token, windows) {
  console.log('\n🏭 DEVOLUCIONES FBA');
  let total = 0;
  for (const w of windows) {
    const rows = await fetchReport('GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA', token, w.start, w.end);
    if (!rows.length) continue;
    if (total === 0 && rows[0]) console.log('     columnas:', Object.keys(rows[0]).join(' | '));

    const toInsert = rows.map(r => {
      const orderId = pick(r, 'order-id', 'Order ID');
      const asin    = pick(r, 'asin', 'ASIN');
      const lpn     = pick(r, 'license-plate-number');
      const rdate   = pick(r, 'return-date');
      return {
        id: hash(`${orderId}|${asin}|${lpn}|${rdate}`),
        return_date: dat(rdate),
        order_id: orderId,
        sku:   pick(r, 'sku'),
        asin,
        fnsku: pick(r, 'fnsku'),
        product_name: pick(r, 'product-name'),
        quantity: int(pick(r, 'quantity')),
        fulfillment_center_id: pick(r, 'fulfillment-center-id'),
        detailed_disposition:  pick(r, 'detailed-disposition'),   // SELLABLE / DAMAGED / ...
        reason:  pick(r, 'reason'),
        status:  pick(r, 'status'),
        license_plate_number: lpn,
        customer_comments: pick(r, 'customer-comments'),
        raw: r,
        updated_at: new Date().toISOString(),
      };
    }).filter(x => x.order_id);

    await upsertBatched('returns_fba', toInsert);
    total += toInsert.length;
    await sleep(2000);
  }
  console.log(`✓ FBA: ${total} devoluciones`);
  return total;
}

// ============================================================
// 3. INDEMNIZACIONES
// ============================================================
async function syncReimbursements(token, windows) {
  console.log('\n💰 INDEMNIZACIONES');
  let total = 0;
  for (const w of windows) {
    let rows = [];
    try {
      rows = await fetchReport('GET_FBA_REIMBURSEMENTS_DATA', token, w.start, w.end);
    } catch (e) {
      console.log('     (no disponible:', e.message.slice(0, 80) + ')');
      continue;
    }
    if (!rows.length) continue;

    const toInsert = rows.map(r => {
      const rid = pick(r, 'reimbursement-id');
      const sku = pick(r, 'sku');
      return {
        id: hash(`${rid}|${sku}|${pick(r, 'approval-date')}`),
        approval_date: dat(pick(r, 'approval-date')),
        reimbursement_id: rid,
        case_id:  pick(r, 'case-id'),
        order_id: pick(r, 'amazon-order-id'),
        reason:   pick(r, 'reason'),
        sku,
        asin:     pick(r, 'asin'),
        product_name: pick(r, 'product-name'),
        amount_per_unit: num(pick(r, 'amount-per-unit')),
        amount_total:    num(pick(r, 'amount-total')),
        qty_cash:      int(pick(r, 'quantity-reimbursed-cash')),
        qty_inventory: int(pick(r, 'quantity-reimbursed-inventory')),
        qty_total:     int(pick(r, 'quantity-reimbursed-total')),
        raw: r,
        updated_at: new Date().toISOString(),
      };
    }).filter(x => x.reimbursement_id);

    await upsertBatched('reimbursements', toInsert);
    total += toInsert.length;
    await sleep(2000);
  }
  console.log(`✓ Indemnizaciones: ${total}`);
  return total;
}

// ============================================================
// 4. TRANSACCIONES (Finances API v2024-06-19 · listTransactions)
//
//    La API antigua (/finances/v0/financialEvents) SOLO devuelve
//    transacciones LIBERADAS. Las DIFERIDAS (que Amazon retiene ~7 días
//    tras la entrega) no aparecen → por eso los reembolsos recientes
//    quedaban sin comisión devuelta.
//
//    listTransactions es el equivalente a la vista "Transacciones" de
//    Seller Central e incluye DEFERRED, RELEASED y DEFERRED_RELEASED.
// ============================================================

// Aplana el árbol de breakdowns en un mapa {tipo: importe}
function flattenBreakdowns(list, out = {}) {
  (list || []).forEach(b => {
    const type = b.breakdownType || 'Unknown';
    const amt = parseFloat(b.breakdownAmount?.currencyAmount ?? 0) || 0;
    out[type] = (out[type] || 0) + amt;
    if (b.breakdowns && b.breakdowns.length) flattenBreakdowns(b.breakdowns, out);
  });
  return out;
}

function relatedId(list, name) {
  const f = (list || []).find(x => x.relatedIdentifierName === name);
  return f ? f.relatedIdentifierValue : null;
}

async function syncTransactions(token, fromISO, toISO) {
  console.log('\n💳 TRANSACCIONES (incluye DIFERIDAS)');
  let all = [], next = null, page = 0;

  do {
    const qs = next
      ? `?nextToken=${encodeURIComponent(next)}`
      : `?postedAfter=${encodeURIComponent(fromISO)}&postedBefore=${encodeURIComponent(toISO)}&marketplaceId=${MARKETPLACE}`;
    const resp = await spApi(`/finances/2024-06-19/transactions${qs}`, token);
    const payload = resp.payload || resp;
    const batch = payload.transactions || [];
    all = all.concat(batch);
    next = payload.nextToken || null;
    page++;
    if (page % 5 === 0 || !next) console.log(`  página ${page}: ${all.length} transacciones`);
    if (next) await sleep(2200);          // rate limit: 0,5 req/s
  } while (next && page < 200);

  console.log(`  ${all.length} transacciones descargadas`);
  if (!all.length) return 0;

  // Diagnóstico: qué tipos y estados hay
  const tipos = {}, estados = {};
  all.forEach(t => {
    tipos[t.transactionType] = (tipos[t.transactionType] || 0) + 1;
    estados[t.transactionStatus] = (estados[t.transactionStatus] || 0) + 1;
  });
  console.log('  tipos:  ', JSON.stringify(tipos));
  console.log('  estados:', JSON.stringify(estados));

  const rows = all.map(t => {
    // el desglose puede venir a nivel de transacción y/o de item
    const bd = flattenBreakdowns(t.breakdowns);
    (t.items || []).forEach(it => flattenBreakdowns(it.breakdowns, bd));

    const orderId = relatedId(t.relatedIdentifiers, 'ORDER_ID')
      || (t.items || []).map(i => relatedId(i.relatedIdentifiers, 'ORDER_ID')).find(Boolean)
      || null;

    const g = (...names) => names.reduce((s, n) => s + (bd[n] || 0), 0);

    return {
      id: t.transactionId || hash(`${orderId}|${t.transactionType}|${t.postedDate}|${t.totalAmount?.currencyAmount}`),
      posted_date: dat(t.postedDate),
      transaction_type: t.transactionType || null,
      transaction_status: t.transactionStatus || null,     // DEFERRED / RELEASED / DEFERRED_RELEASED
      order_id: orderId,
      description: t.description || null,
      total_amount:      parseFloat(t.totalAmount?.currencyAmount ?? 0) || 0,
      product_charges:   g('ProductCharges', 'Product charges', 'Principal'),
      promo_rebates:     g('PromotionalRebates', 'Promotional rebates'),
      amazon_fees:       g('AmazonFees', 'Amazon fees'),
      other_adjustments: g('Other', 'OtherAdjustments', 'Other adjustments'),
      shipping_charges:  g('ShippingCharges', 'Shipping charges'),
      breakdowns: bd,
      raw: t,
      updated_at: new Date().toISOString(),
    };
  });

  await upsertBatched('transactions', rows);
  console.log(`✓ ${rows.length} transacciones guardadas`);
  return rows.length;
}

// ============================================================
// VENTANAS DE 60 DÍAS
// ============================================================
function buildWindows(fromDate, toDate) {
  const out = [];
  let cur = new Date(fromDate);
  const end = new Date(toDate);
  while (cur < end) {
    const wEnd = new Date(Math.min(cur.getTime() + 55 * 86400000, end.getTime()));
    out.push({ start: cur.toISOString(), end: wEnd.toISOString() });
    cur = new Date(wEnd.getTime() + 1000);
  }
  return out;
}

// ============================================================
// MAIN
// ============================================================
async function main() {
  console.log('🚀 Sincronización de DEVOLUCIONES → Supabase');
  console.log('Fecha:', new Date().toISOString());

  const arg = process.argv.find(a => a.startsWith('--desde='));
  const desde = arg ? arg.split('=')[1] : null;

  const hasta = new Date(Date.now() - 30 * 60 * 1000);   // 30 min de margen
  const inicio = desde
    ? new Date(desde + 'T00:00:00Z')
    : new Date(Date.now() - 55 * 86400000);              // por defecto: últimos 55 días

  const windows = buildWindows(inicio, hasta);
  console.log(`Rango: ${inicio.toISOString().slice(0, 10)} → ${hasta.toISOString().slice(0, 10)}`);
  console.log(`Ventanas de 60 días: ${windows.length}`);

  const token = await getAccessToken();
  console.log('✓ Token Amazon obtenido');

  const res = { fbm: 0, fba: 0, reimb: 0, tx: 0 };

  try { res.fbm = await syncReturnsFbm(token, windows); }
  catch (e) { console.error('✗ Error FBM:', e.message); }

  try { res.fba = await syncReturnsFba(token, windows); }
  catch (e) { console.error('✗ Error FBA:', e.message); }

  try { res.reimb = await syncReimbursements(token, windows); }
  catch (e) { console.error('✗ Error indemnizaciones:', e.message); }

  try { res.tx = await syncTransactions(token, inicio.toISOString(), hasta.toISOString()); }
  catch (e) { console.error('✗ Error transacciones:', e.message); }

  await supabase('POST', 'returns_sync_log', [{
    last_sync: new Date().toISOString(),
    report_type: 'ALL',
    rows_synced: res.fbm + res.fba + res.reimb + res.tx,
    status: 'ok',
    notes: `FBM:${res.fbm} FBA:${res.fba} Indem:${res.reimb} Transacciones:${res.tx}`,
  }], '', 'return=minimal').catch(e => console.log('log warning:', e.message));

  console.log('\n✅ Completado');
  console.log(`   Devoluciones FBM:  ${res.fbm}`);
  console.log(`   Devoluciones FBA:  ${res.fba}`);
  console.log(`   Indemnizaciones:   ${res.reimb}`);
  console.log(`   Transacciones:     ${res.tx}`);
}

main().catch(err => { console.error('❌ Error fatal:', err); process.exit(1); });
