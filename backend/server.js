import http from 'node:http'
import { URL } from 'node:url'
import { Pool } from 'pg'

const PORT = Number(process.env.PORT ?? 3001)
const SUPABASE_DSN = process.env.SUPABASE_DSN

if (!SUPABASE_DSN) {
  throw new Error('SUPABASE_DSN is required.')
}

const pool = new Pool({
  connectionString: SUPABASE_DSN,
  ssl: { rejectUnauthorized: false },
})

const INVENTORY_QUERY = `
select coalesce(
  json_agg(row_to_json(product_rows) order by product_rows.name),
  '[]'::json
) as inventory
from (
  select
    p.id,
    p.name,
    p.sku,
    p.barcode,
    p.category,
    p.cost_price_gbp,
    p.sell_price_gbp,
    p.buy_cost_exc_vat_gbp,
    p.trade_price_ex_vat_gbp,
    p.trade_price_ex_vat_usd,
    p.quoted_usd_per_gbp,
    p.fx_quote_date,
    p.markup,
    coalesce(
      json_agg(
        json_build_object(
          'warehouse', ws.warehouse,
          'qty_on_hand', ws.qty_on_hand,
          'qty_reserved', ws.qty_reserved,
          'reorder_point', ws.reorder_point
        )
        order by ws.warehouse
      ) filter (where ws.id is not null),
      '[]'::json
    ) as warehouse_stock
  from products p
  left join warehouse_stock ws on ws.product_id = p.id
  group by
    p.id,
    p.name,
    p.sku,
    p.barcode,
    p.category,
    p.cost_price_gbp,
    p.sell_price_gbp,
    p.buy_cost_exc_vat_gbp,
    p.trade_price_ex_vat_gbp,
    p.trade_price_ex_vat_usd,
    p.quoted_usd_per_gbp,
    p.fx_quote_date,
    p.markup
) as product_rows;
`

function writeJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  })
  response.end(JSON.stringify(payload))
}

function normalizeApiPath(pathname) {
  const withoutTrailingSlash = pathname.replace(/\/+$/, '') || '/'

  if (withoutTrailingSlash === '/api') {
    return '/'
  }

  if (withoutTrailingSlash.startsWith('/api/')) {
    return withoutTrailingSlash.slice(4)
  }

  return withoutTrailingSlash
}

async function readJsonBody(request) {
  return await new Promise((resolve, reject) => {
    const chunks = []
    let size = 0

    request.on('data', (chunk) => {
      size += chunk.length
      if (size > 1024 * 1024) {
        reject(new Error('request_body_too_large'))
        request.destroy()
        return
      }
      chunks.push(chunk)
    })

    request.on('end', () => {
      if (chunks.length === 0) {
        resolve({})
        return
      }

      const bodyText = Buffer.concat(chunks).toString('utf8')
      try {
        resolve(JSON.parse(bodyText))
      } catch {
        reject(new Error('invalid_json'))
      }
    })

    request.on('error', (error) => {
      reject(error)
    })
  })
}

function toNumber(value) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function toInteger(value) {
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) ? parsed : 0
}

function deriveOrderStatus(lines) {
  if (!lines.length) {
    return 'confirmed'
  }

  const statuses = lines.map((line) => line.fulfilmentStatus)

  if (statuses.every((status) => status === 'fulfilled')) {
    return 'fulfilled'
  }

  if (statuses.every((status) => status === 'backordered' || status === 'unmatched')) {
    return 'backordered'
  }

  if (statuses.some((status) => status === 'partial' || status === 'backordered' || status === 'unmatched')) {
    return 'partially_fulfilled'
  }

  return 'confirmed'
}

async function listCustomers(searchTerm) {
  const normalizedSearch = String(searchTerm ?? '').trim()

  const { rows } = await pool.query(
    `
      select
        c.id,
        c.company_name,
        c.contact_name,
        c.email,
        c.billing_address,
        c.currency_preference,
        c.vat_number,
        c.notes,
        c.created_at,
        coalesce(count(o.id), 0)::int as order_count,
        coalesce(round(sum(o.total_gbp)::numeric, 2), 0)::numeric(12,2) as total_spend_gbp
      from customers c
      left join orders o on o.customer_id = c.id
      where ($1 = '' or c.company_name ilike $2)
      group by c.id
      order by c.company_name asc
    `,
    [normalizedSearch, `%${normalizedSearch}%`],
  )

  return rows
}

async function createCustomer(payload) {
  const companyName = String(payload.companyName ?? '').trim()
  if (!companyName) {
    throw new Error('company_name_required')
  }

  const currencyPreference = String(payload.currencyPreference ?? 'GBP').toUpperCase()
  const allowedCurrencies = new Set(['GBP', 'USD', 'EUR'])
  const safeCurrency = allowedCurrencies.has(currencyPreference) ? currencyPreference : 'GBP'

  const { rows } = await pool.query(
    `
      insert into customers (
        company_name,
        contact_name,
        email,
        billing_address,
        currency_preference,
        vat_number,
        notes
      )
      values ($1, $2, $3, $4, $5, $6, $7)
      returning *
    `,
    [
      companyName,
      payload.contactName ? String(payload.contactName).trim() : null,
      payload.email ? String(payload.email).trim() : null,
      payload.billingAddress ? String(payload.billingAddress).trim() : null,
      safeCurrency,
      payload.vatNumber ? String(payload.vatNumber).trim() : null,
      payload.notes ? String(payload.notes).trim() : null,
    ],
  )

  return rows[0]
}

async function getCustomer(customerId) {
  const { rows } = await pool.query(
    `
      select
        c.*,
        coalesce(count(o.id), 0)::int as order_count,
        coalesce(round(sum(o.total_gbp)::numeric, 2), 0)::numeric(12,2) as total_spend_gbp
      from customers c
      left join orders o on o.customer_id = c.id
      where c.id = $1
      group by c.id
      limit 1
    `,
    [customerId],
  )

  return rows[0] ?? null
}

async function updateCustomer(customerId, payload) {
  const fieldMap = [
    ['companyName', 'company_name'],
    ['contactName', 'contact_name'],
    ['email', 'email'],
    ['billingAddress', 'billing_address'],
    ['currencyPreference', 'currency_preference'],
    ['vatNumber', 'vat_number'],
    ['notes', 'notes'],
  ]

  const updates = []
  const values = []

  for (const [inputField, columnName] of fieldMap) {
    if (!Object.prototype.hasOwnProperty.call(payload, inputField)) {
      continue
    }

    let value = payload[inputField]

    if (value === undefined) {
      continue
    }

    if (columnName === 'currency_preference') {
      value = String(value ?? 'GBP').toUpperCase()
      const allowedCurrencies = new Set(['GBP', 'USD', 'EUR'])
      value = allowedCurrencies.has(value) ? value : 'GBP'
    } else if (value !== null) {
      value = String(value).trim()
      if (value.length === 0 && columnName !== 'company_name') {
        value = null
      }
    }

    if (columnName === 'company_name' && (!value || String(value).trim() === '')) {
      throw new Error('company_name_required')
    }

    values.push(value)
    updates.push(`${columnName} = $${values.length}`)
  }

  if (!updates.length) {
    const customer = await getCustomer(customerId)
    return customer
  }

  values.push(customerId)

  const { rows } = await pool.query(
    `
      update customers
      set ${updates.join(', ')}
      where id = $${values.length}
      returning *
    `,
    values,
  )

  return rows[0] ?? null
}

async function listCustomerOrders(customerId) {
  const { rows } = await pool.query(
    `
      select
        o.id,
        o.order_ref,
        o.status,
        o.currency,
        o.total_gbp,
        o.created_at,
        coalesce(sum(ol.qty_ordered), 0)::int as total_units,
        count(ol.id)::int as line_count
      from orders o
      left join order_lines ol on ol.order_id = o.id
      where o.customer_id = $1
      group by o.id
      order by o.created_at desc
    `,
    [customerId],
  )

  return rows
}

async function createOrder(payload) {
  const customerId = payload.customerId ? String(payload.customerId).trim() : ''
  const customerName = String(payload.customerName ?? '').trim()
  const currency = String(payload.currency ?? 'GBP').toUpperCase()
  const notes = payload.notes ? String(payload.notes).trim() : null
  const lines = Array.isArray(payload.lines) ? payload.lines : []

  if (!customerId) {
    throw new Error('customer_id_required')
  }

  if (!customerName) {
    throw new Error('customer_name_required')
  }

  if (!lines.length) {
    throw new Error('order_lines_required')
  }

  const normalizedLines = lines.map((line) => {
    const qtyOrdered = toInteger(line.qtyOrdered)
    const qtyFulfilled = toInteger(line.qtyFulfilled)
    const qtyBackordered = toInteger(line.qtyBackordered)
    const unitPriceGbp = toNumber(line.unitPriceGbp)
    const fulfilmentStatus = String(line.fulfilmentStatus ?? 'unmatched')

    return {
      productId: line.productId ? String(line.productId) : null,
      productName: String(line.productName ?? '').trim() || 'Unknown Product',
      sku: line.sku ? String(line.sku).trim() : null,
      ean: line.ean ? String(line.ean).trim() : null,
      qtyOrdered,
      qtyFulfilled,
      qtyBackordered,
      unitPriceGbp,
      warehouseAllocated: line.warehouseAllocated ? String(line.warehouseAllocated) : null,
      fulfilmentStatus,
    }
  })

  const totalGbp = normalizedLines.reduce((sum, line) => sum + line.qtyOrdered * line.unitPriceGbp, 0)
  const status = deriveOrderStatus(normalizedLines)
  const safeCurrency = ['GBP', 'USD', 'EUR'].includes(currency) ? currency : 'GBP'

  const client = await pool.connect()
  try {
    await client.query('begin')

    const currentYear = new Date().getUTCFullYear()
    await client.query('select pg_advisory_xact_lock(hashtext($1))', [`orders:${currentYear}`])

    const { rows: refRows } = await client.query(
      `
        select
          coalesce(max((substring(order_ref from 'ORD-\\d{4}-(\\d+)'))::int), 0) as last_sequence
        from orders
        where order_ref like $1
          and order_ref ~ '^ORD-\\d{4}-\\d+$'
      `,
      [`ORD-${currentYear}-%`],
    )

    const lastSequence = Number(refRows[0]?.last_sequence ?? 0)
    const nextSequence = lastSequence + 1
    const orderRef = `ORD-${currentYear}-${String(nextSequence).padStart(4, '0')}`

    const { rows: orderRows } = await client.query(
      `
        insert into orders (
          order_ref,
          customer_id,
          customer_name,
          status,
          currency,
          fx_rate,
          total_gbp,
          notes
        )
        values ($1, $2, $3, $4, $5, 1.0, $6, $7)
        returning *
      `,
      [orderRef, customerId, customerName, status, safeCurrency, totalGbp.toFixed(2), notes],
    )

    const order = orderRows[0]

    for (const line of normalizedLines) {
      await client.query(
        `
          insert into order_lines (
            order_id,
            product_id,
            product_name,
            sku,
            ean,
            qty_ordered,
            qty_fulfilled,
            qty_backordered,
            unit_price_gbp,
            warehouse_allocated,
            fulfilment_status
          )
          values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `,
        [
          order.id,
          line.productId,
          line.productName,
          line.sku,
          line.ean,
          line.qtyOrdered,
          line.qtyFulfilled,
          line.qtyBackordered,
          line.unitPriceGbp.toFixed(2),
          line.warehouseAllocated,
          line.fulfilmentStatus,
        ],
      )
    }

    await client.query('commit')

    return {
      id: order.id,
      orderRef: order.order_ref,
      status: order.status,
      totalGbp: toNumber(order.total_gbp),
      lineCount: normalizedLines.length,
    }
  } catch (error) {
    await client.query('rollback')
    throw error
  } finally {
    client.release()
  }
}

const server = http.createServer(async (request, response) => {
  const method = request.method ?? 'GET'
  const requestUrl = new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`)
  const routePath = normalizeApiPath(requestUrl.pathname)

  if (method === 'GET' && routePath === '/health') {
    writeJson(response, 200, { ok: true })
    return
  }

  if (method === 'GET' && routePath === '/inventory') {
    try {
      const { rows } = await pool.query(INVENTORY_QUERY)
      const inventory = rows[0]?.inventory ?? []
      writeJson(response, 200, inventory)
      return
    } catch (error) {
      console.error('Inventory query failed:', error)
      writeJson(response, 500, { error: 'inventory_query_failed' })
      return
    }
  }

  if (method === 'GET' && routePath === '/customers') {
    try {
      const searchTerm = requestUrl.searchParams.get('search') ?? ''
      const customers = await listCustomers(searchTerm)
      writeJson(response, 200, customers)
      return
    } catch (error) {
      console.error('Customer list query failed:', error)
      writeJson(response, 500, { error: 'customer_list_query_failed' })
      return
    }
  }

  if (method === 'POST' && routePath === '/customers') {
    try {
      const payload = await readJsonBody(request)
      const customer = await createCustomer(payload)
      writeJson(response, 201, customer)
      return
    } catch (error) {
      if (error instanceof Error && error.message === 'company_name_required') {
        writeJson(response, 400, { error: 'company_name_required' })
        return
      }

      if (error instanceof Error && error.message === 'invalid_json') {
        writeJson(response, 400, { error: 'invalid_json' })
        return
      }

      if (error instanceof Error && error.message === 'request_body_too_large') {
        writeJson(response, 413, { error: 'request_body_too_large' })
        return
      }

      console.error('Create customer failed:', error)
      writeJson(response, 500, { error: 'create_customer_failed' })
      return
    }
  }

  const customerOrdersMatch = routePath.match(/^\/customers\/([^/]+)\/orders$/)
  if (method === 'GET' && customerOrdersMatch) {
    const customerId = customerOrdersMatch[1]
    try {
      const orders = await listCustomerOrders(customerId)
      writeJson(response, 200, orders)
      return
    } catch (error) {
      console.error('Customer orders query failed:', error)
      writeJson(response, 500, { error: 'customer_orders_query_failed' })
      return
    }
  }

  const customerInvoicesMatch = routePath.match(/^\/customers\/([^/]+)\/invoices$/)
  if (method === 'GET' && customerInvoicesMatch) {
    writeJson(response, 200, [])
    return
  }

  const customerMatch = routePath.match(/^\/customers\/([^/]+)$/)
  if (customerMatch && method === 'GET') {
    const customerId = customerMatch[1]
    try {
      const customer = await getCustomer(customerId)
      if (!customer) {
        writeJson(response, 404, { error: 'customer_not_found' })
        return
      }

      writeJson(response, 200, customer)
      return
    } catch (error) {
      console.error('Customer lookup failed:', error)
      writeJson(response, 500, { error: 'customer_lookup_failed' })
      return
    }
  }

  if (customerMatch && method === 'PATCH') {
    const customerId = customerMatch[1]
    try {
      const payload = await readJsonBody(request)
      const customer = await updateCustomer(customerId, payload)
      if (!customer) {
        writeJson(response, 404, { error: 'customer_not_found' })
        return
      }

      writeJson(response, 200, customer)
      return
    } catch (error) {
      if (error instanceof Error && error.message === 'company_name_required') {
        writeJson(response, 400, { error: 'company_name_required' })
        return
      }

      if (error instanceof Error && error.message === 'invalid_json') {
        writeJson(response, 400, { error: 'invalid_json' })
        return
      }

      if (error instanceof Error && error.message === 'request_body_too_large') {
        writeJson(response, 413, { error: 'request_body_too_large' })
        return
      }

      console.error('Update customer failed:', error)
      writeJson(response, 500, { error: 'update_customer_failed' })
      return
    }
  }

  if (method === 'POST' && routePath === '/orders') {
    try {
      const payload = await readJsonBody(request)
      const order = await createOrder(payload)
      writeJson(response, 201, order)
      return
    } catch (error) {
      if (
        error instanceof Error &&
        ['customer_id_required', 'customer_name_required', 'order_lines_required', 'invalid_json'].includes(
          error.message,
        )
      ) {
        writeJson(response, 400, { error: error.message })
        return
      }

      if (error instanceof Error && error.message === 'request_body_too_large') {
        writeJson(response, 413, { error: 'request_body_too_large' })
        return
      }

      console.error('Create order failed:', error)
      writeJson(response, 500, { error: 'create_order_failed' })
      return
    }
  }

  if (method === 'POST' && routePath === '/parse-unstructured') {
    try {
      const openrouterKey = process.env.OPENROUTER_KEY
      if (!openrouterKey) {
        writeJson(response, 500, { error: 'openrouter_key_not_configured' })
        return
      }

      const payload = await readJsonBody(request)
      const text = String(payload.text ?? '').trim()

      if (!text) {
        writeJson(response, 400, { error: 'text_required' })
        return
      }

      const systemPrompt = `You are an invoice data extraction assistant. Extract structured order data from unstructured text.

Return ONLY valid JSON with this exact structure, no markdown, no explanation:
{
  "metadata": {
    "customerName": "string or empty",
    "orderNumber": "string or empty",
    "orderDate": "YYYY-MM-DD or empty"
  },
  "lines": [
    {
      "productCode": "SKU/product code",
      "description": "product name/description",
      "barcode": "EAN/barcode or empty",
      "tradePrice": 0.00,
      "tradeQuantity": 1
    }
  ]
}

Rules:
- Extract every product/item mentioned with a price or quantity
- tradePrice must be a number (no currency symbols)
- tradeQuantity must be an integer, default to 1 if not specified
- If no barcode is found, use empty string
- If no SKU/product code is found, generate a short code from the product name
- Extract customer name, order number, and date from headers/metadata if present
- Return ONLY the JSON object, nothing else`

      const aiResponse = await fetch('https://openrouter.ai/api/v1/chat/completions', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${openrouterKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: 'openrouter/elephant-alpha',
          messages: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: text },
          ],
          temperature: 0.1,
          max_tokens: 8000,
        }),
        signal: AbortSignal.timeout(60000),
      })

      if (!aiResponse.ok) {
        const errorText = await aiResponse.text()
        console.error('OpenRouter API error:', aiResponse.status, errorText)
        writeJson(response, 502, { error: 'ai_request_failed' })
        return
      }

      const aiResult = await aiResponse.json()
      const content = aiResult.choices?.[0]?.message?.content ?? ''

      const jsonMatch = content.match(/\{[\s\S]*\}/)
      if (!jsonMatch) {
        writeJson(response, 422, { error: 'ai_response_not_parseable', raw: content.slice(0, 500) })
        return
      }

      let parsed
      try {
        parsed = JSON.parse(jsonMatch[0])
      } catch {
        writeJson(response, 422, { error: 'ai_response_invalid_json', raw: content.slice(0, 500) })
        return
      }

      const lines = Array.isArray(parsed.lines) ? parsed.lines : []
      const normalizedLines = lines
        .map((line) => ({
          productCode: String(line.productCode ?? '').trim(),
          description: String(line.description ?? '').trim(),
          barcode: String(line.barcode ?? '').trim(),
          tradePrice: toNumber(line.tradePrice),
          tradeQuantity: Math.max(1, toInteger(line.tradeQuantity)),
          subTotal: toNumber(line.tradePrice) * Math.max(1, toInteger(line.tradeQuantity)),
        }))
        .filter((line) => line.description || line.productCode)

      const metadata = {
        customerName: String(parsed.metadata?.customerName ?? '').trim(),
        orderNumber: String(parsed.metadata?.orderNumber ?? '').trim(),
        orderDate: String(parsed.metadata?.orderDate ?? '').trim(),
      }

      writeJson(response, 200, { metadata, lines: normalizedLines })
      return
    } catch (error) {
      if (error instanceof Error && error.message === 'invalid_json') {
        writeJson(response, 400, { error: 'invalid_json' })
        return
      }

      console.error('Parse unstructured failed:', error)
      writeJson(response, 500, { error: 'parse_unstructured_failed' })
      return
    }
  }

  if (method === 'POST' && routePath === '/invoices') {
    try {
      const payload = await readJsonBody(request)
      const customerName = String(payload.customerName ?? '').trim()
      const customerRegion = String(payload.customerRegion ?? '').toLowerCase()
      const orderNumber = String(payload.orderNumber ?? '').trim()
      const orderDate = payload.orderDate || null
      const invoiceDate = payload.invoiceDate || new Date().toISOString().slice(0, 10)
      const sourceFilename = String(payload.sourceFilename ?? '').trim()
      const lines = Array.isArray(payload.lines) ? payload.lines : []

      if (!customerName) {
        writeJson(response, 400, { error: 'customer_name_required' })
        return
      }

      if (!lines.length) {
        writeJson(response, 400, { error: 'lines_required' })
        return
      }

      const validRegions = new Set(['uk', 'us', 'eu'])
      const safeRegion = validRegions.has(customerRegion) ? customerRegion : null

      const client = await pool.connect()
      try {
        await client.query('begin')

        const { rows: customerRows } = await client.query(
          `
            insert into customers (company_name, region)
            values ($1, $2)
            on conflict (company_name) do update set
              region = coalesce(excluded.region, customers.region)
            returning id
          `,
          [customerName, safeRegion],
        )
        const customerId = customerRows[0].id

        let totalUnits = 0
        let totalValue = 0
        for (const line of lines) {
          const qty = toInteger(line.quantity)
          const subtotal = toNumber(line.subtotal)
          totalUnits += qty
          totalValue += subtotal
        }

        const { rows: invoiceRows } = await client.query(
          `
            insert into invoices (
              customer_id, order_number, order_date, invoice_date,
              total_units, total_value_gbp, source_filename
            )
            values ($1, $2, $3, $4, $5, $6, $7)
            returning id
          `,
          [
            customerId,
            orderNumber || null,
            orderDate || null,
            invoiceDate,
            totalUnits,
            totalValue.toFixed(2),
            sourceFilename || null,
          ],
        )
        const invoiceId = invoiceRows[0].id

        const warehouseForRegion = safeRegion === 'us' ? 'us' : 'uk'
        const fulfilmentCounts = { fulfilled: 0, partial: 0, backordered: 0 }

        for (const line of lines) {
          const sku = String(line.sku ?? '').trim()
          const qty = toInteger(line.quantity)
          const { rows: productRows } = await client.query(
            'select id from products where sku = $1 limit 1',
            [sku],
          )
          const productId = productRows[0]?.id ?? null

          let fulfilmentStatus = 'backordered'
          let qtyFulfilled = 0
          let qtyBackordered = qty

          if (productId) {
            const { rows: stockRows } = await client.query(
              `select id, qty_on_hand, qty_reserved
               from warehouse_stock
               where product_id = $1 and warehouse = $2
               for update`,
              [productId, warehouseForRegion],
            )

            if (stockRows.length > 0) {
              const stock = stockRows[0]
              const available = stock.qty_on_hand - stock.qty_reserved

              if (available >= qty) {
                qtyFulfilled = qty
                qtyBackordered = 0
                fulfilmentStatus = 'fulfilled'
                await client.query(
                  'update warehouse_stock set qty_reserved = qty_reserved + $1 where id = $2',
                  [qty, stock.id],
                )
              } else if (available > 0) {
                qtyFulfilled = available
                qtyBackordered = qty - available
                fulfilmentStatus = 'partial'
                await client.query(
                  'update warehouse_stock set qty_reserved = qty_reserved + $1 where id = $2',
                  [available, stock.id],
                )
              }
            }
          }

          fulfilmentCounts[fulfilmentStatus] += 1

          await client.query(
            `
              insert into invoice_lines (
                invoice_id, product_id, sku, description, barcode,
                trade_price_gbp, quantity, subtotal_gbp,
                fulfilment_status, qty_fulfilled, qty_backordered
              )
              values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            `,
            [
              invoiceId,
              productId,
              sku,
              String(line.description ?? '').trim(),
              String(line.barcode ?? '').trim() || null,
              toNumber(line.tradePrice).toFixed(2),
              qty,
              toNumber(line.subtotal).toFixed(2),
              fulfilmentStatus,
              qtyFulfilled,
              qtyBackordered,
            ],
          )
        }

        await client.query('commit')
        writeJson(response, 201, {
          invoiceId,
          lineCount: lines.length,
          fulfilmentSummary: fulfilmentCounts,
        })
      } catch (innerError) {
        await client.query('rollback')

        if (innerError.code === '23505') {
          writeJson(response, 409, { error: 'duplicate_invoice' })
          return
        }

        throw innerError
      } finally {
        client.release()
      }

      return
    } catch (error) {
      if (error instanceof Error && error.message === 'invalid_json') {
        writeJson(response, 400, { error: 'invalid_json' })
        return
      }

      console.error('Create invoice failed:', error)
      writeJson(response, 500, { error: 'create_invoice_failed' })
      return
    }
  }

  if (method === 'GET' && routePath === '/backorders') {
    try {
      const { rows } = await pool.query(`
        select
          il.id as line_id,
          il.sku,
          il.description,
          il.quantity,
          il.qty_fulfilled,
          il.qty_backordered,
          il.fulfilment_status,
          il.trade_price_gbp,
          il.created_at as line_created_at,
          i.id as invoice_id,
          i.order_number,
          i.invoice_date,
          c.id as customer_id,
          c.company_name,
          c.region,
          coalesce(loyalty.total_spend, 0)::numeric(12,2) as customer_total_spend
        from invoice_lines il
        join invoices i on il.invoice_id = i.id
        left join customers c on i.customer_id = c.id
        left join lateral (
          select sum(inv.total_value_gbp) as total_spend
          from invoices inv
          where inv.customer_id = c.id
        ) loyalty on true
        where il.fulfilment_status in ('backordered', 'partial')
        order by il.sku asc, coalesce(loyalty.total_spend, 0) desc
      `)
      writeJson(response, 200, rows)
      return
    } catch (error) {
      console.error('Backorders query failed:', error)
      writeJson(response, 500, { error: 'backorders_query_failed' })
      return
    }
  }

  if (method === 'GET' && routePath === '/stretch/customer-health') {
    try {
      const { rows } = await pool.query(`
        with customer_invoices as (
          select
            i.customer_id,
            c.company_name,
            i.invoice_date,
            i.total_value_gbp
          from invoices i
          join customers c on c.id = i.customer_id
          where i.customer_id is not null
            and i.invoice_date is not null
        ),
        invoice_sequences as (
          select
            customer_id,
            company_name,
            invoice_date,
            total_value_gbp,
            lag(invoice_date) over (
              partition by customer_id
              order by invoice_date
            ) as previous_invoice_date
          from customer_invoices
        ),
        customer_totals as (
          select
            customer_id,
            max(company_name) as company_name,
            count(*)::int as invoice_count,
            coalesce(sum(total_value_gbp), 0)::numeric(12,2) as total_revenue_gbp,
            max(invoice_date) as last_invoice_date
          from customer_invoices
          group by customer_id
        ),
        cadence as (
          select
            customer_id,
            avg((invoice_date - previous_invoice_date)::numeric) as avg_reorder_days_raw
          from invoice_sequences
          where previous_invoice_date is not null
          group by customer_id
        ),
        scored as (
          select
            totals.customer_id,
            totals.company_name,
            totals.total_revenue_gbp,
            totals.invoice_count,
            coalesce(cadence.avg_reorder_days_raw, 30) as avg_reorder_days_raw,
            totals.last_invoice_date,
            (current_date - totals.last_invoice_date)::int as days_since_last_invoice,
            least(
              120,
              greatest(
                14,
                round(coalesce(cadence.avg_reorder_days_raw, 30))::int
              )
            ) as cadence_days
          from customer_totals totals
          left join cadence on cadence.customer_id = totals.customer_id
          where totals.invoice_count >= 2
        ),
        classified as (
          select
            customer_id,
            company_name,
            total_revenue_gbp,
            invoice_count,
            round(avg_reorder_days_raw::numeric, 1) as avg_reorder_days,
            last_invoice_date,
            days_since_last_invoice,
            (last_invoice_date + cadence_days) as expected_next_invoice_date,
            least(120, greatest(30, ceil(cadence_days * 1.5)::int)) as lapse_threshold_days,
            case
              when days_since_last_invoice > least(120, greatest(30, ceil(cadence_days * 1.5)::int))
                then 'lapsed'
              when days_since_last_invoice > cadence_days
                then 'due_soon'
              else 'healthy'
            end as risk_status
          from scored
        )
        select
          customer_id,
          company_name,
          total_revenue_gbp,
          invoice_count,
          avg_reorder_days,
          last_invoice_date,
          days_since_last_invoice,
          expected_next_invoice_date,
          lapse_threshold_days,
          risk_status,
          row_number() over (
            order by
              case risk_status
                when 'lapsed' then 1
                when 'due_soon' then 2
                else 3
              end,
              total_revenue_gbp desc
          )::int as priority_rank
        from classified
        order by
          case risk_status
            when 'lapsed' then 1
            when 'due_soon' then 2
            else 3
          end,
          total_revenue_gbp desc
      `)

      const normalizedRows = rows.map((row) => ({
        customer_id: row.customer_id,
        company_name: row.company_name,
        total_revenue_gbp: toNumber(row.total_revenue_gbp),
        invoice_count: toInteger(row.invoice_count),
        avg_reorder_days: toNumber(row.avg_reorder_days),
        last_invoice_date: row.last_invoice_date,
        days_since_last_invoice: toInteger(row.days_since_last_invoice),
        expected_next_invoice_date: row.expected_next_invoice_date,
        lapse_threshold_days: toInteger(row.lapse_threshold_days),
        risk_status: String(row.risk_status ?? 'healthy'),
        priority_rank: toInteger(row.priority_rank),
      }))

      writeJson(response, 200, normalizedRows)
      return
    } catch (error) {
      console.error('Stretch customer health query failed:', error)
      writeJson(response, 500, { error: 'stretch_customer_health_query_failed' })
      return
    }
  }

  if (method === 'GET' && routePath === '/stretch/summary') {
    try {
      const [totalsResult, topResult, bottomResult, monthlyResult, customersResult, mismatchResult] =
        await Promise.all([
          pool.query(`
            select
              count(*)::int as total_invoices,
              coalesce(sum(total_value_gbp), 0)::numeric(12,2) as total_revenue,
              coalesce(sum(total_units), 0)::int as total_units,
              min(invoice_date) as earliest_date,
              max(invoice_date) as latest_date
            from invoices
          `),
          pool.query(`
            select sku, description, sum(quantity)::int as total_units,
              sum(subtotal_gbp)::numeric(12,2) as total_revenue,
              count(distinct invoice_id)::int as invoice_count
            from invoice_lines
            group by sku, description
            order by sum(quantity) desc
            limit 10
          `),
          pool.query(`
            select sku, description, sum(quantity)::int as total_units,
              sum(subtotal_gbp)::numeric(12,2) as total_revenue,
              count(distinct invoice_id)::int as invoice_count
            from invoice_lines
            group by sku, description
            order by sum(quantity) asc
            limit 10
          `),
          pool.query(`
            select to_char(invoice_date, 'YYYY-MM') as month,
              sum(total_value_gbp)::numeric(12,2) as revenue,
              sum(total_units)::int as units,
              count(*)::int as invoice_count
            from invoices
            group by to_char(invoice_date, 'YYYY-MM')
            order by month asc
          `),
          pool.query(`
            select c.company_name, c.region,
              count(i.id)::int as invoice_count,
              coalesce(sum(i.total_value_gbp), 0)::numeric(12,2) as total_revenue,
              coalesce(sum(i.total_units), 0)::int as total_units
            from customers c
            inner join invoices i on i.customer_id = c.id
            group by c.id, c.company_name, c.region
            order by sum(i.total_value_gbp) desc
          `),
          pool.query(`
            select distinct il.sku, il.description, c.region as customer_region,
              coalesce(ws_uk.qty_on_hand, 0)::int as uk_stock,
              coalesce(ws_us.qty_on_hand, 0)::int as us_stock
            from invoice_lines il
            join invoices i on il.invoice_id = i.id
            join customers c on i.customer_id = c.id
            left join products p on il.sku = p.sku
            left join warehouse_stock ws_uk on p.id = ws_uk.product_id and ws_uk.warehouse = 'uk'
            left join warehouse_stock ws_us on p.id = ws_us.product_id and ws_us.warehouse = 'us'
            where c.region is not null
              and (
                (c.region = 'us' and coalesce(ws_us.qty_on_hand, 0) = 0)
                or (c.region = 'uk' and coalesce(ws_uk.qty_on_hand, 0) = 0)
              )
            order by il.sku
          `),
        ])

      const totals = totalsResult.rows[0]

      writeJson(response, 200, {
        totalInvoices: totals.total_invoices,
        totalRevenue: toNumber(totals.total_revenue),
        totalUnits: totals.total_units,
        dateRange: {
          earliest: totals.earliest_date,
          latest: totals.latest_date,
        },
        topProducts: topResult.rows,
        bottomProducts: bottomResult.rows,
        revenueByMonth: monthlyResult.rows,
        customerBreakdown: customersResult.rows,
        warehouseMismatch: mismatchResult.rows,
      })
      return
    } catch (error) {
      console.error('Stretch summary query failed:', error)
      writeJson(response, 500, { error: 'stretch_summary_query_failed' })
      return
    }
  }

  writeJson(response, 404, { error: 'not_found' })
})

server.listen(PORT, () => {
  console.log(`Telemachus live API listening on :${PORT}`)
})

async function shutdown() {
  server.close()
  await pool.end()
  process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
