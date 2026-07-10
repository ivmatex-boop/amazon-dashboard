/**
 * sync-amazon.js
 * Sincronización incremental Amazon SP-API → Supabase
 * Se ejecuta via GitHub Actions (cron nocturno o manual)
 *
 * CAMBIO CLAVE (fix precios a 0):
 *   Antes se pedían pedidos por CreatedAfter (fecha de creación) → un pedido que
 *   entra en Pending nunca se volvía a leer al confirmarse, y su precio se quedaba a 0.
 *   Ahora se piden por LastUpdatedAfter (fecha de última modificación) → cuando un pedido
 *   pasa de Pending a Shipped/Unshipped, vuelve a entrar en la sincronización y se
 *   reescribe con su precio real.
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
  const watermark = new Date(rawDate).toISOString();

  // Ventana por FECHA DE ÚLTIMA MODIFICACIÓN (no de creación).
  // Solapamiento de seguridad de 10 min para no perder cambios justo en el borde;
  // el guardado es idempotente (merge-duplicates), así que repetir no molesta.
  const lastUpdatedAfter = new Date(new Date(watermark).getTime() - 10 * 60 * 1000).toISOString();

  console.log(`Última sincronización: ${lastSync?.last_sync}`);
  console.log(`Marca guardada: ${watermark}`);
  console.log(`Trayendo pedidos modificados desde: ${lastUpdatedAfter}`);

  // 2. Auth Amazon
  const token = await getAccessToken();
  console.log('✓ Token Amazon obtenido');

  // 3. Cargar pedidos modificados paginando
  let allOrders = [];
  let nextToken = null;
  let page = 0;
  const dateTo = new Date(Date.now() - 15 * 60 * 1000).toISOString();
  // Esta será la nueva marca al terminar: el límite superior de la ventana.
  const newWatermark = dateTo;

  do {
    const url = nextToken
      ? `/orders/v0/orders?NextToken=${encodeURIComponent(nextToken)}`
      : `/orders/v0/orders?MarketplaceIds=${MARKETPLACE}&LastUpdatedAfter=${encodeURIComponent(lastUpdatedAfter)}&LastUpdatedBefore=${encodeURIComponent(dateTo)}&OrderStatuses=Shipped,Unshipped,PartiallyShipped,Pending,Canceled,Unfulfillable,InvoiceUnconfirmed,PendingAvailability`;

    const data = await spApi(url, token);
    if (data.errors) { console.error('Error pedidos:', data.errors); break; }

    const batch = data.payload?.Orders || [];
    allOrders = allOrders.concat(batch);
    nextToken = data.payload?.NextToken || null;
    page++;
    console.log(`  Página ${page}: ${batch.length} pedidos (total: ${allOrders.length})`);

    if (nextToken) await sleep(600);
  } while (nextToken && page < 100);

  console.log(`✓ ${allOrders.length} pedidos nuevos/modificados encontrados`);

  if (allOrders.length === 0) {
    console.log('No hay pedidos modificados. Sincronización completada.');
    await supabase('POST', 'sync_log', {
      marketplace: 'ES', last_sync: new Date().toISOString(),
      last_order_date: newWatermark, orders_synced: 0, status: 'ok', notes: 'Sin cambios'
    });
    return;
  }

  // 4. Guardar pedidos en Supabase (upsert: actualiza estado y total al reprocesar)
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
  //    Al reprocesar un pedido que ya estaba en Pending, se borran sus items antiguos
  //    (precio 0) y se reinsertan con el ItemPrice real ahora que está confirmado.
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

  // 6b + 6c. Cargar eventos financieros con paginación completa
  console.log('Cargando eventos financieros (paginación completa)...');
  // Usar siempre desde inicio del año para capturar todos los datos
  const finDateFromFull = '2026-01-01T00:00:00Z';

  let allShipmentEvents = [];
  let allRefundEvents = [];
  let allAdEvents = [];
  let allServiceFees = [];
  let allAdjustments = [];
  let allSafetEvents = [];
  let finNextToken = null;
  let finPage = 0;

  try {
    do {
      const finUrl = finNextToken
        ? `/finances/v0/financialEvents?NextToken=${encodeURIComponent(finNextToken)}`
        : `/finances/v0/financialEvents?PostedAfter=${encodeURIComponent(finDateFromFull)}&PostedBefore=${encodeURIComponent(dateTo)}`;

      const finResp = await spApi(finUrl, token);
      const ev = finResp.payload?.FinancialEvents || {};

      allShipmentEvents = allShipmentEvents.concat(ev.ShipmentEventList || []);
      allRefundEvents = allRefundEvents.concat(ev.RefundEventList || []);
      allAdEvents = allAdEvents.concat(ev.ProductAdsPaymentEventList || []);
      allServiceFees = allServiceFees.concat(ev.ServiceFeeEventList || []);
      allAdjustments = allAdjustments.concat(ev.AdjustmentEventList || []);
      allSafetEvents = allSafetEvents.concat(ev.SAFETReimbursementEventList || []);

      finNextToken = finResp.payload?.NextToken || null;
      finPage++;
      console.log(`  Página ${finPage}: ${(ev.ShipmentEventList||[]).length} envíos, ${(ev.RefundEventList||[]).length} reembolsos`);
      if (finNextToken) await sleep(500);
    } while (finNextToken && finPage < 50);

    console.log(`✓ Finanzas: ${allShipmentEvents.length} envíos, ${allRefundEvents.length} reembolsos, ${allAdEvents.length} publicidad`);
  } catch(e) {
    console.error('Error cargando eventos financieros:', e.message);
  }

  // Guardar fees desde eventos financieros
  if (allShipmentEvents.length > 0) {
    const feesMap2 = {};
    allShipmentEvents.forEach(ev => {
      const orderId = ev.AmazonOrderId;
      if (!orderId) return;
      let totalFee = 0;
      (ev.ShipmentItemList || []).forEach(item => {
        (item.ItemFeeList || []).forEach(fee => {
          totalFee += Math.abs(parseFloat(fee.FeeAmount?.CurrencyAmount || 0));
        });
      });
      if (totalFee > 0) feesMap2[orderId] = (feesMap2[orderId] || 0) + totalFee;
    });
    const feesToInsert2 = Object.entries(feesMap2).map(([order_id, total_fee]) => ({
      order_id, total_fee, is_estimated: false, updated_at: new Date().toISOString()
    }));
    if (feesToInsert2.length > 0) {
      // Borrar fees existentes y reinsertar
      await supabase('DELETE', 'order_fees', null, '?order_id=neq.null').catch(() => {});
      for (let i = 0; i < feesToInsert2.length; i += 100) {
        await supabase('POST', 'order_fees', feesToInsert2.slice(i, i + 100));
      }
      console.log(`✓ ${feesToInsert2.length} fees reales guardados desde eventos financieros`);
    }
  }

  // Guardar reembolsos
  if (allRefundEvents.length > 0) {
    const refundsToInsert = allRefundEvents.map(ev => {
      let amount = 0;
      (ev.ShipmentItemAdjustmentList || []).forEach(item => {
        (item.ItemChargeAdjustmentList || []).forEach(charge => {
          if (charge.ChargeType === 'Principal') {
            amount += Math.abs(parseFloat(charge.ChargeAmount?.CurrencyAmount || 0));
          }
        });
      });
      return { order_id: ev.AmazonOrderId || null, amount, currency: 'EUR', posted_date: ev.PostedDate || new Date().toISOString(), raw: ev };
    }).filter(r => r.amount > 0);

    if (refundsToInsert.length > 0) {
      await supabase('DELETE', 'refunds', null, '?order_id=neq.null').catch(() => {});
      await supabase('POST', 'refunds', refundsToInsert);
      console.log(`✓ ${refundsToInsert.length} reembolsos guardados`);
    }
  }

  // Procesar gastos adicionales
  console.log('Procesando gastos adicionales...');
  try {
    const finEvents = { ProductAdsPaymentEventList: allAdEvents, ServiceFeeEventList: allServiceFees, AdjustmentEventList: allAdjustments, SAFETReimbursementEventList: allSafetEvents };
    const periodMonth = new Date(watermark).toISOString().slice(0, 7); // YYYY-MM

    // Publicidad
    const adEvents = finEvents.ProductAdsPaymentEventList || [];
    if (adEvents.length > 0) {
      await supabase('DELETE', 'amazon_charges', null, `?charge_type=eq.advertising&period_month=eq.${periodMonth}`).catch(() => {});
      const adCharges = adEvents.map(ev => ({
        marketplace: 'ES',
        charge_type: 'advertising',
        description: `Publicidad Amazon ${periodMonth}`,
        amount: Math.abs(parseFloat(ev.transactionValue?.CurrencyAmount || ev.baseValue?.CurrencyAmount || 0)),
        currency: 'EUR',
        posted_date: ev.postedDate || new Date().toISOString(),
        period_month: periodMonth,
        raw: ev
      })).filter(c => c.amount > 0);
      if (adCharges.length > 0) {
        await supabase('POST', 'amazon_charges', adCharges);
        console.log(`  ✓ ${adCharges.length} cargos publicidad guardados (total: €${adCharges.reduce((s,c)=>s+c.amount,0).toFixed(2)})`);
      }
    }

    // Cuota suscripción y almacenamiento FBA
    const serviceFees = finEvents.ServiceFeeEventList || [];
    const feeCharges = [];
    serviceFees.forEach(ev => {
      (ev.FeeList || []).forEach(fee => {
        const amount = Math.abs(parseFloat(fee.FeeAmount?.CurrencyAmount || 0));
        if (amount > 0) {
          feeCharges.push({
            marketplace: 'ES',
            charge_type: fee.FeeType === 'Subscription' ? 'subscription' : 'fba_storage',
            description: ev.FeeDescription || fee.FeeType,
            amount,
            currency: fee.FeeAmount?.CurrencyCode || 'EUR',
            posted_date: new Date().toISOString(),
            period_month: periodMonth,
            raw: ev
          });
        }
      });
    });
    if (feeCharges.length > 0) {
      await supabase('DELETE', 'amazon_charges', null, `?charge_type=in.(subscription,fba_storage)&period_month=eq.${periodMonth}`).catch(() => {});
      await supabase('POST', 'amazon_charges', feeCharges);
      console.log(`  ✓ ${feeCharges.length} cargos fijos guardados`);
    }

    // Cargos por portes de devolución (AdjustmentEventList con ReturnPostageBilling)
    const adjustments = finEvents.AdjustmentEventList || [];
    const postageAdjustments = adjustments.filter(ev =>
      ev.AdjustmentType?.includes('ReturnPostageBilling_Postage') ||
      ev.AdjustmentType?.includes('ReturnPostageBilling_VAT')
    );
    if (postageAdjustments.length > 0) {
      const postageToInsert = postageAdjustments.map(ev => ({
        order_id: null, // No viene vinculado a pedido en este endpoint
        amount: Math.abs(parseFloat(ev.AdjustmentAmount?.CurrencyAmount || 0)),
        currency: ev.AdjustmentAmount?.CurrencyCode || 'EUR',
        posted_date: ev.PostedDate || new Date().toISOString(),
        raw: ev
      })).filter(p => p.amount > 0);
      if (postageToInsert.length > 0) {
        await supabase('POST', 'return_postage', postageToInsert);
        console.log(`  ✓ ${postageToInsert.length} cargos portes devolución guardados`);
      }
    }

    // Reembolsos SAFET
    const safetEvents = finEvents.SAFETReimbursementEventList || [];
    if (safetEvents.length > 0) {
      const safetToInsert = safetEvents.map(ev => ({
        marketplace: 'ES',
        claim_id: ev.SAFETClaimId,
        amount: Math.abs(parseFloat(ev.ReimbursedAmount?.CurrencyAmount || 0)),
        currency: ev.ReimbursedAmount?.CurrencyCode || 'EUR',
        reason_code: ev.ReasonCode,
        product_description: ev.SAFETReimbursementItemList?.[0]?.productDescription || '',
        posted_date: ev.PostedDate || new Date().toISOString(),
        raw: ev
      })).filter(s => s.amount > 0);
      if (safetToInsert.length > 0) {
        await supabase('POST', 'safet_reimbursements', safetToInsert);
        console.log(`  ✓ ${safetToInsert.length} reembolsos SAFET guardados (total: €${safetToInsert.reduce((s,r)=>s+r.amount,0).toFixed(2)})`);
      }
    }

    // Ajustes FBA (otros ajustes no relacionados con portes)
    const fbaAdjustments = adjustments.filter(ev =>
      !ev.AdjustmentType?.includes('ReturnPostageBilling')
    );
    if (fbaAdjustments.length > 0) {
      const fbaToInsert = fbaAdjustments.map(ev => ({
        marketplace: 'ES',
        adjustment_type: ev.AdjustmentType,
        amount: parseFloat(ev.AdjustmentAmount?.CurrencyAmount || 0),
        currency: ev.AdjustmentAmount?.CurrencyCode || 'EUR',
        posted_date: ev.PostedDate || new Date().toISOString(),
        raw: ev
      })).filter(f => f.amount !== 0);
      if (fbaToInsert.length > 0) {
        await supabase('POST', 'fba_adjustments', fbaToInsert);
        console.log(`  ✓ ${fbaToInsert.length} ajustes FBA guardados`);
      }
    }

  } catch(e) {
    console.error('Error procesando gastos adicionales:', e.message);
  }

  // 7. Actualizar log de sincronización.
  //    La nueva marca es el límite superior de la ventana (patrón LastUpdatedAfter),
  //    no la fecha de compra máxima, para que la siguiente ejecución continúe sin huecos.
  await supabase('POST', 'sync_log', [{
    marketplace: 'ES',
    last_sync: new Date().toISOString(),
    last_order_date: newWatermark,
    orders_synced: allOrders.length,
    status: 'ok',
    notes: `${allOrders.length} pedidos · ${itemsProcessed} con items · ${feesProcessed} con fees`
  }]).catch(e => console.log('sync_log warning:', e.message));

  console.log('');
  console.log('✅ Sincronización completada');
  console.log(`   Pedidos: ${allOrders.length}`);
  console.log(`   Items: ${itemsProcessed}`);
  console.log(`   Fees: ${feesProcessed}`);
  console.log(`   Próxima ventana desde: ${newWatermark}`);
}

main().catch(err => {
  console.error('❌ Error fatal:', err);
  process.exit(1);
});
