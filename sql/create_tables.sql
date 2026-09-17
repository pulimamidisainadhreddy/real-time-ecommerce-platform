CREATE TABLE IF NOT EXISTS public.online_retail (
    invoice_no VARCHAR(50) NOT NULL,
    stock_code VARCHAR(50) NOT NULL,
    description TEXT,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price DOUBLE PRECISION,
    customer_id VARCHAR(50),
    country VARCHAR(100),
    transaction_type VARCHAR(20),
    revenue DOUBLE PRECISION,

    CONSTRAINT online_retail_unique_record
    UNIQUE (
        invoice_no,
        stock_code,
        description,
        quantity,
        invoice_date,
        unit_price,
        customer_id,
        country
    )
);


CREATE INDEX IF NOT EXISTS idx_online_retail_invoice_date
ON public.online_retail (invoice_date);

CREATE INDEX IF NOT EXISTS idx_online_retail_customer_id
ON public.online_retail (customer_id);

CREATE INDEX IF NOT EXISTS idx_online_retail_stock_code
ON public.online_retail (stock_code);

CREATE INDEX IF NOT EXISTS idx_online_retail_transaction_type
ON public.online_retail (transaction_type);