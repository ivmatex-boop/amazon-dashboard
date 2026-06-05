/**
 * sync-amazon.js
 * Sincronización incremental Amazon SP-API → Supabase
 * Se ejecuta via GitHub Actions (cron nocturno o manual)
 */

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SECRET = process.env.SUPABASE_SECRET;
const AMAZON_CLIENT_ID = process.env.AMAZON_CLIENT_ID;
const AMAZON_CLIENT_SECRET = process.env.AMAZON_CLIENT_SECRET;
const AMAZON_REFRESH_TOKEN = process.env.AMAZON_REFRESH_TOKEN;
const MARKETPLACE = 'A1RKKUPIHCS9HS';
const BASE = 'https://sellingpartnerapi-eu.amazon.com';

// ============================================================
// SUPABASE HELPERS
// ============================================================
async function supabase(method, table, data = null, query = '') {
  const url = `${SUPABASE_URL}/rest/v1/${table}${query}`;
  const res = await fetch(url, {
    method,
    headers: {
      'apikey': SUPABASE_SECRET,
      'Authorization': `Bearer ${SUPABASE_SECRET}`,
      'Content-Type': 'application/json',
      'Prefer': method === 'POST' ? 'resolution=merge-duplicates' : '',
    },
    body: data ? JSON.stringify(data) : undefined,
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Supabase ${method} ${table}: ${res.status} ${err}`);
  }
  if (res.status === 204 || res.headers.get('content-length') === '0') return null;
  const text = await res.text();
  if (!text || text.trim() === '') return null;
  return JSON.parse(text);
}

// ============================================================
// AMAZON AUTH
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

// ============================================================
// AMAZON SP-API CALLS
// ============================================================
async function spApi(path, token) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'x-amz-access-token': token, 'Content-Type': 'application/json' }
  });
  if (res.status === 429) {
    console.log('Rate limited, esperando 2s...');
    await sleep(2000);
    return spApi(path, token);
  }
  return res.json();
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ============================================================
// SINCRONIZACIÓN PRINCIPAL
// ============================================================
async function main() {
  console.log('🚀 Iniciando sincronización Amazon → Supabase');
  console.log('Fecha:', new Date().toISOString());

  // 1. Obtener última sincronización
  const syncLogs = await supabase('GET', 'sync_log', null, '?marketplace=eq.ES&order=last_sync.desc&limit=1');
  const lastSync = syncLogs?.[0];
  const rawDate = lastSync?.last_order_date || '2026-01-01T00:00:00Z';
  // Normalizar a formato Z (Amazon requiere ISO8601 con Z, no +00:00)
  const lastOrderDate = new Date(rawDate).toISOString();
  console.log(`Última sincronización: ${lastSync?.last_sync}`);
  console.log(`Cargando pedidos desde: ${lastOrderDate}`);

  // 2. Auth Amazon
  const token = await getAccessToken();
  console.log('✓ Token Amazon obtenido');

  // 3. Cargar pedidos nuevos paginando
  let allOrders = [];
  let nextToken = null;
  let page = 0;
  const dateTo = new Date(Date.now() - 15 * 60 * 1000).toISOString();

  do {
    const url = nextToken
      ? `/orders/v0/orders?NextToken=${encodeURIComponent(nextToken)}`
      : `/orders/v0/orders?MarketplaceIds=${MARKETPLACE}&CreatedAfter=${encodeURIComponent(lastOrderDate)}&CreatedBefore=${encodeURIComponent(dateTo)}&OrderStatuses=Shipped,Unshipped,PartiallyShipped,Pending,Canceled,Unfulfillable,InvoiceUnconfirmed,PendingAvailability`;

    const data = await spApi(url, token);
    if (data.errors) { console.error('Error pedidos:', data.errors); break; }

    const batch = data.payload?.Orders || [];
    allOrders = allOrders.concat(batch);
    nextToken = data.payload?.NextToken || null;
    page++;
    console.log(`  Página ${page}: ${batch.length} pedidos (total: ${allOrders.length})`);

    if (nextToken) await sleep(600);
  } while (nextToken && page < 100);

  console.log(`✓ ${allOrders.length} pedidos nuevos encontrados`);

  if (allOrders.length === 0) {
    console.log('No hay pedidos nuevos. Sincronización completada.');
    await supabase('POST', 'sync_log', {
      marketplace: 'ES', last_sync: new Date().toISOString(),
      last_order_date: lastOrderDate, orders_synced: 0, status: 'ok', notes: 'Sin pedidos nuevos'
    });
    return;
  }

  // 4. Guardar pedidos en Supabase
  console.log('Guardando pedidos en Supabase...');
  const ordersToInsert = allOrders.map(o => ({
    id: o.AmazonOrderId,
    marketplace: 'ES',
    purchase_date: o.PurchaseDate,
    status: o.OrderStatus,
    fulfillment_channel: o.FulfillmentChannel,
    amount: parseFloat(o.OrderTotal?.Amount || 0),
    currency: o.OrderTotal?.CurrencyCode || 'EUR',
    num_items_shipped: o.NumberOfItemsShipped || 0,
    raw: o,
    updated_at: new Date().toISOString()
  }));

  // Insertar en batches de 100
  for (let i = 0; i < ordersToInsert.length; i += 100) {
    const batch = ordersToInsert.slice(i, i + 100);
    await supabase('POST', 'orders', batch);
    console.log(`  Guardados ${Math.min(i + 100, ordersToInsert.length)}/${ordersToInsert.length} pedidos`);
  }
  console.log('✓ Pedidos guardados');

  // 5. Cargar y guardar items de cada pedido
  const validOrders = allOrders.filter(o => o.OrderStatus !== 'Canceled');
  console.log(`Cargando items de ${validOrders.length} pedidos válidos...`);

  let itemsProcessed = 0;
  for (let i = 0; i < validOrders.length; i += 5) {
    const batch = validOrders.slice(i, i + 5);

    await Promise.all(batch.map(async o => {
      try {
        const data = await spApi(`/orders/v0/orders/${o.AmazonOrderId}/orderItems`, token);
        const items = data.payload?.OrderItems || [];

        if (items.length > 0) {
          // Borrar items anteriores del pedido (por si acaso)
          await supabase('DELETE', 'order_items', null, `?order_id=eq.${o.AmazonOrderId}`);

          const itemsToInsert = items.map(item => ({
            order_id: o.AmazonOrderId,
            asin: item.ASIN,
            sku: item.SellerSKU,
            title: item.Title,
            quantity: item.QuantityOrdered || 1,
            item_price: parseFloat(item.ItemPrice?.Amount || 0),
            raw: item
          }));
          await supabase('POST', 'order_items', itemsToInsert);
        }
        itemsProcessed++;
      } catch(e) {
        console.error(`Error items pedido ${o.AmazonOrderId}:`, e.message);
      }
    }));

    if (i % 50 === 0) console.log(`  Items: ${Math.min(i + 5, validOrders.length)}/${validOrders.length} pedidos`);
    await sleep(400);
  }
  console.log(`✓ Items guardados (${itemsProcessed} pedidos procesados)`);

  // 6. Cargar fees reales por pedido
  console.log('Cargando fees reales...');
  let feesProcessed = 0;
  for (let i = 0; i < validOrders.length; i += 5) {
    const batch = validOrders.slice(i, i + 5);
    await Promise.all(batch.map(async o => {
      try {
        const data = await spApi(`/finances/v0/financialEvents/orderId/${o.AmazonOrderId}`, token);
        const events = data.payload?.FinancialEvents || {};
        let totalFee = 0;
        let hasData = false;
        (events.ShipmentEventList || []).forEach(ev => {
          (ev.ShipmentItemList || []).forEach(item => {
            (item.ItemFeeList || []).forEach(fee => {
              totalFee += Math.abs(parseFloat(fee.FeeAmount?.Amount || 0));
              hasData = true;
            });
          });
        });
        if (hasData) {
          await supabase('POST', 'order_fees', {
            order_id: o.AmazonOrderId,
            total_fee: totalFee,
            is_estimated: false,
            updated_at: new Date().toISOString()
          });
          feesProcessed++;
        }
      } catch(e) { /* fee no disponible aún */ }
    }));
    await sleep(300);
  }
  console.log(`✓ Fees guardados (${feesProcessed} pedidos con fees reales)`);

  // 7. Actualizar log de sincronización
  const maxDate = allOrders.reduce((max, o) => o.PurchaseDate > max ? o.PurchaseDate : max, lastOrderDate);
  await supabase('POST', 'sync_log', [{
    marketplace: 'ES',
    last_sync: new Date().toISOString(),
    last_order_date: maxDate,
    orders_synced: allOrders.length,
    status: 'ok',
    notes: `${allOrders.length} pedidos · ${itemsProcessed} con items · ${feesProcessed} con fees`
  }]).catch(e => console.log('sync_log warning:', e.message));

  console.log('');
  console.log('✅ Sincronización completada');
  console.log(`   Pedidos: ${allOrders.length}`);
  console.log(`   Items: ${itemsProcessed}`);
  console.log(`   Fees: ${feesProcessed}`);
  console.log(`   Próxima desde: ${maxDate}`);
}

main().catch(err => {
  console.error('❌ Error fatal:', err);
  process.exit(1);
});
