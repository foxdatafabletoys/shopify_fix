-- Phase 3: Backorder tracking on invoice_lines
-- Uses existing warehouse_stock.qty_reserved (defined but never populated until now).

ALTER TABLE invoice_lines ADD COLUMN IF NOT EXISTS fulfilment_status text DEFAULT 'fulfilled';
ALTER TABLE invoice_lines ADD COLUMN IF NOT EXISTS qty_fulfilled integer NOT NULL DEFAULT 0;
ALTER TABLE invoice_lines ADD COLUMN IF NOT EXISTS qty_backordered integer NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_invoice_lines_fulfilment_status ON invoice_lines(fulfilment_status);

-- Fix historical data so existing lines are consistent
UPDATE invoice_lines SET qty_fulfilled = quantity WHERE fulfilment_status = 'fulfilled' AND qty_fulfilled = 0;
