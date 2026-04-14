# Backorders System

## Overview

The Backorders page displays items where customer demand exceeds available stock and automatically prioritizes allocation based on customer loyalty.

## Purpose

When multiple customers order the same SKU but inventory is insufficient to fulfill all requests, the system needs to decide which customers get their orders fulfilled first. This is critical for:

- **Revenue protection**: High-value customers receive priority service
- **Churn prevention**: Loyal customers stay satisfied during stockouts
- **Fair allocation**: Objective ranking replaces subjective decision-making
- **Visibility**: Clear view of unfulfilled demand and its value

## How It Works

### Data Source

The system fetches backorder data from `/api/backorders`, which returns:

```json
[
  {
    "line_id": "unique-line-identifier",
    "sku": "PRODUCT-SKU",
    "description": "Product Name",
    "customer_id": "CUST-123",
    "company_name": "Customer Company Ltd",
    "region": "UK",
    "customer_total_spend": 45000.00,
    "qty_fulfilled": 10,
    "qty_backordered": 5,
    "trade_price_gbp": 12.50,
    "fulfilment_status": "partial",
    "order_number": "ORD-2024-001",
    "invoice_date": "2024-03-15"
  }
]
```

### Customer Ranking Logic

Customers are ranked by **total lifetime spend** (`customer_total_spend`). Higher spend = higher priority.

This means:
- A customer who has spent £50,000 total gets priority over one who has spent £5,000
- The ranking is objective and automatic
- No manual intervention needed to decide allocation priority

### Display Structure

**Summary Cards** show:
- Total number of backordered SKUs
- Total backordered units across all products
- Number of unique customers waiting for stock
- Total monetary value of backordered items

**SKU Groups**: Backorders are grouped by SKU, with each product showing:
- SKU code and description
- Total units backordered across all customers
- Table of all customers waiting for that SKU

**Customer Priority Table** for each SKU displays:

| Column | Description |
|--------|-------------|
| **Priority** | Ranked position (1 = highest priority) |
| **Customer** | Company name |
| **Region** | Customer region (UK/US/etc) |
| **Loyalty (Spend)** | Total lifetime spend in GBP |
| **Qty Fulfilled** | Units already allocated from available stock |
| **Qty Backordered** | Units still waiting for stock |
| **Status** | "Partial" (some fulfilled) or "Backordered" (none fulfilled) |
| **Order #** | Reference number for the order |
| **Date** | Invoice date |

## Workflow

### 1. Order Entry
When an order is saved in the Invoicing page, the system:
- Checks available stock for each line item
- Allocates stock where available
- Creates backorder records for shortfalls

### 2. Customer Ranking
The backend ranks customers by `customer_total_spend` (descending):
- Customer A: £50,000 ’ Priority 1
- Customer B: £30,000 ’ Priority 2
- Customer C: £10,000 ’ Priority 3

### 3. Stock Allocation
When stock becomes available:
- Priority 1 customer gets allocated first
- Remaining stock goes to Priority 2
- Process continues down the priority list

### 4. Visibility
The Backorders page shows:
- Which products are in shortage
- Which customers are waiting
- Who gets served first when stock arrives
- Total financial impact of stockouts

## Use Cases

### Restock Decision Making
"We have 50 units of SKU-001 arriving. The Backorders page shows 5 customers need it, totaling 75 units backordered. Priority customers are Company A (30 units, £80K spend) and Company B (25 units, £45K spend). We fulfill Company A completely and give 20 units to Company B."

### Proactive Communication
"Our top customer (£120K lifetime spend) has 15 units backordered of SKU-789. We can contact them with an ETA before they have to chase us."

### Demand Planning
"SKU-456 has consistent backorders from 8 different customers totaling £12,000 in unfulfilled value. This is a clear signal to increase reorder quantity."

### Stockout Triage
"During a supply shortage, instead of random allocation or 'first come first served,' we systematically serve our most valuable customers first."

## Business Logic

### Partial Fulfillment
If a customer orders 20 units but only 12 are available:
- `qty_fulfilled`: 12
- `qty_backordered`: 8
- `fulfilment_status`: "partial"

### Full Backorder
If a customer orders 20 units but 0 are available:
- `qty_fulfilled`: 0
- `qty_backordered`: 20
- `fulfilment_status`: "backordered"

### Priority Calculation
Priority is determined by:
1. **Primary**: Total customer spend (higher = better priority)
2. **Display**: Sorted within each SKU group, numbered 1, 2, 3...

## Data Aggregation

The page performs two levels of grouping:

### Summary Level
Aggregates across all backorders:
- Count unique SKUs with backorders
- Sum all backordered units
- Count unique customers affected
- Calculate total value (qty × trade price)

### SKU Level
Groups backorder lines by product:
- Each SKU gets its own table
- Shows total backordered quantity for that SKU
- Lists all customers waiting, sorted by priority

## Empty State

When there are no backorders:
> "No backorders. All invoiced items are fully allocated from available stock."

This indicates healthy inventory levels or perfect demand/supply matching.

## Technical Implementation

### Frontend
- Fetches from `/api/backorders` with cache-busting
- Groups raw data by SKU using Map
- Calculates summary statistics with useMemo
- Formats currency with Intl.NumberFormat (en-GB, GBP)

### Expected Backend Behavior
The API should:
- Query invoice line items where `qty_backordered > 0`
- Join with customer data to get `company_name` and `customer_total_spend`
- Join with product data for `description` and `trade_price_gbp`
- Sort results by `customer_total_spend DESC` within each SKU
- Return as JSON array

## Future Enhancements

Potential improvements:
- **Auto-notification**: Email customers when their backorders can be fulfilled
- **ETA tracking**: Show expected restock dates per SKU
- **Manual override**: Allow admin to boost specific customer priority
- **Trend analysis**: Show which products are chronically backordered
- **Allocation automation**: Auto-create picking lists when stock arrives, prioritized by customer rank
