-- warehouse_functions.sql


-- Tabel: warehouses
CREATE TABLE IF NOT EXISTS warehouses (
  warehouse_id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  location TEXT
);

-- Tabel: products
CREATE TABLE IF NOT EXISTS products (
  product_id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  category_id INT, -- Foreign Key ke tabel categories 
  supplier_id INT, -- Foreign Key ke tabel suppliers 
  unit_price DECIMAL(10, 2) NOT NULL -- Harga beli dasar
);

-- Tabel: stocks (Current stock levels per product per warehouse)
CREATE TABLE IF NOT EXISTS stocks (
  stock_id SERIAL PRIMARY KEY,
  product_id INT NOT NULL REFERENCES products(product_id),
  warehouse_id INT NOT NULL REFERENCES warehouses(warehouse_id),
  quantity INT NOT NULL DEFAULT 0 CHECK (quantity >= 0),
  last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (product_id, warehouse_id) 
);

-- Tabel: stock_movements (Log semua pergerakan stok)
CREATE TABLE IF NOT EXISTS stock_movements (
  movement_id SERIAL PRIMARY KEY,
  product_id INT NOT NULL REFERENCES products(product_id),
  warehouse_id INT NOT NULL REFERENCES warehouses(warehouse_id),
  movement_type VARCHAR(20) NOT NULL, -- 'IN', 'OUT', 'ADJUSTMENT_IN', 'ADJUSTMENT_OUT', 'TRANSFER'
  quantity INT NOT NULL CHECK (quantity > 0), -- Selalu positif; movement_type mendefinisikan IN/OUT
  movement_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  reference_type VARCHAR(20), -- e.g., 'PURCHASE_ORDER', 'SALES_ORDER', 'ADJUSTMENT', 'TRANSFER'
  reference_id INT, -- ID dari PO/SO/Transfer yang terkait
  notes TEXT
);

-- Tabel: stock_receipt_costs (Untuk melacak biaya per batch masuk, penting untuk valuasi FIFO/LIFO/AVG)
CREATE TABLE IF NOT EXISTS stock_receipt_costs (
    receipt_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES products(product_id),
    warehouse_id INT NOT NULL REFERENCES warehouses(warehouse_id),
    movement_id INT REFERENCES stock_movements(movement_id), -- Referensi ke movement IN (opsional tapi disarankan)
    quantity_received INT NOT NULL CHECK (quantity_received > 0),
    unit_cost DECIMAL(10, 2) NOT NULL,
    receipt_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    remaining_quantity INT NOT NULL CHECK (remaining_quantity >= 0) -- Untuk melacak sisa kuantitas dari batch ini
);

-- Tabel: reorder_points (Untuk setting batas stok minimum dan maksimum)
CREATE TABLE IF NOT EXISTS reorder_points (
    reorder_point_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES products(product_id),
    warehouse_id INT REFERENCES warehouses(warehouse_id), -- Bisa per gudang atau global (NULL)
    min_stock_level INT NOT NULL CHECK (min_stock_level >= 0),
    max_stock_level INT NOT NULL CHECK (max_stock_level >= 0),
    UNIQUE (product_id, warehouse_id) -- Hanya satu reorder point per produk per gudang
);

-- Tabel: audit_log (Untuk mencatat semua perubahan penting pada tabel, misalnya 'stocks')
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    record_id INT, -- ID dari record yang berubah (misal: stock_id)
    operation_type VARCHAR(10) NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE'
    old_data JSONB, -- Data sebelum perubahan
    new_data JSONB, -- Data setelah perubahan
    changed_by TEXT DEFAULT CURRENT_USER,
    changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------------------
-- 2. Contoh Data (Opsional, untuk pengujian)
-- Hapus bagian ini jika Anda tidak ingin data dummy terbuat.
-- --------------------------------------------------------

-- Masukkan data gudang
INSERT INTO warehouses (name, location) VALUES
('Gudang Utama', 'Jakarta'),
('Gudang Cabang A', 'Surabaya')
ON CONFLICT (warehouse_id) DO NOTHING; -- Hindari duplikasi jika sudah ada

-- Masukkan data produk
INSERT INTO products (name, unit_price) VALUES
('Acer Predator RGB', 1200.00),
('Razer Wireless Mouse', 45.00),
('Asus Keyboard', 80.00),
('Sony Smart TV', 450.00)
ON CONFLICT (product_id) DO NOTHING;

-- Masukkan data stok awal (hanya jika belum ada)
INSERT INTO stocks (product_id, warehouse_id, quantity) VALUES
(1, 1, 10), 
(2, 1, 50), 
(3, 1, 20), 
(1, 2, 5)    
ON CONFLICT (product_id, warehouse_id) DO NOTHING;

-- Masukkan reorder points
INSERT INTO reorder_points (product_id, warehouse_id, min_stock_level, max_stock_level) VALUES
(1, 1, 5, 20),
(2, 1, 20, 100),
(3, 1, 10, 50),
(1, 2, 2, 10)
ON CONFLICT (product_id, warehouse_id) DO NOTHING;

-- --------------------------------------------------------
-- 3. Database Functions / Stored Procedures
-- --------------------------------------------------------

-- 3.1 Stock Movement Function
-- Function untuk mencatat pergerakan stok dan memperbarui stok saat ini
CREATE OR REPLACE FUNCTION record_stock_movement(
    p_product_id INT,
    p_warehouse_id INT,
    p_movement_type VARCHAR(20), -- 'IN', 'OUT', 'ADJUSTMENT_IN', 'ADJUSTMENT_OUT'
    p_quantity INT,
    p_reference_type VARCHAR(20) DEFAULT NULL, -- e.g., 'PURCHASE_ORDER', 'SALES_ORDER', 'ADJUSTMENT'
    p_reference_id INT DEFAULT NULL,
    p_notes TEXT DEFAULT NULL,
    p_unit_cost DECIMAL(10, 2) DEFAULT NULL -- Diperlukan untuk movement_type 'IN'
) RETURNS JSON AS $$
DECLARE
    v_current_stock INT;
    v_new_stock INT;
    v_movement_id INT;
    v_response JSON;
    v_effect_quantity INT;
BEGIN
    -- Validasi p_quantity
    IF p_quantity <= 0 THEN
        RETURN json_build_object('status', 'error', 'message', 'Quantity must be positive.');
    END IF;

    -- Validasi p_movement_type
    IF p_movement_type NOT IN ('IN', 'OUT', 'ADJUSTMENT_IN', 'ADJUSTMENT_OUT') THEN
        RETURN json_build_object('status', 'error', 'message', 'Invalid movement type. Allowed: IN, OUT, ADJUSTMENT_IN, ADJUSTMENT_OUT.');
    END IF;

    -- Dapatkan stok saat ini, lock row untuk menghindari race condition
    SELECT quantity INTO v_current_stock
    FROM stocks
    WHERE product_id = p_product_id AND warehouse_id = p_warehouse_id
    FOR UPDATE;

    IF v_current_stock IS NULL THEN
        -- Jika produk belum ada di gudang, inisialisasi jika movement_type adalah 'IN' atau 'ADJUSTMENT_IN'
        IF p_movement_type IN ('IN', 'ADJUSTMENT_IN') THEN
            INSERT INTO stocks (product_id, warehouse_id, quantity)
            VALUES (p_product_id, p_warehouse_id, 0)
            RETURNING quantity INTO v_current_stock;
        ELSE
            RETURN json_build_object('status', 'error', 'message', 'Product not found in warehouse for OUT/ADJUSTMENT_OUT movement.');
        END IF;
    END IF;

    -- Hitung stok baru
    IF p_movement_type IN ('IN', 'ADJUSTMENT_IN') THEN
        v_effect_quantity := p_quantity;
        v_new_stock := v_current_stock + p_quantity;
    ELSE -- 'OUT', 'ADJUSTMENT_OUT'
        v_effect_quantity := -p_quantity;
        v_new_stock := v_current_stock - p_quantity;
    END IF;

    -- Periksa ketersediaan stok untuk movement 'OUT'
    IF v_new_stock < 0 THEN
        RETURN json_build_object('status', 'error', 'message', 'Insufficient stock for ' || p_movement_type || ' movement. Current: ' || v_current_stock || ', Requested: ' || p_quantity);
    END IF;

    -- Perbarui tabel stocks
    UPDATE stocks
    SET quantity = v_new_stock,
        last_updated = CURRENT_TIMESTAMP
    WHERE product_id = p_product_id AND warehouse_id = p_warehouse_id;

    -- Catat pergerakan di stock_movements
    INSERT INTO stock_movements (
        product_id, warehouse_id, movement_type, quantity,
        reference_type, reference_id, notes
    ) VALUES (
        p_product_id, p_warehouse_id, p_movement_type, p_quantity,
        p_reference_type, p_reference_id, p_notes
    ) RETURNING movement_id INTO v_movement_id;

    -- Jika movement_type adalah 'IN', catat juga di stock_receipt_costs
    IF p_movement_type = 'IN' AND p_unit_cost IS NOT NULL THEN
        INSERT INTO stock_receipt_costs (
            product_id, warehouse_id, movement_id, quantity_received, unit_cost, remaining_quantity
        ) VALUES (
            p_product_id, p_warehouse_id, v_movement_id, p_quantity, p_unit_cost, p_quantity
        );
    ELSIF p_movement_type = 'OUT' THEN

        NULL;
    END IF;

    v_response := json_build_object(
        'status', 'success',
        'message', 'Stock movement recorded and current stock updated.',
        'movement_id', v_movement_id,
        'product_id', p_product_id,
        'warehouse_id', p_warehouse_id,
        'old_stock', v_current_stock,
        'new_stock', v_new_stock,
        'effect_quantity', v_effect_quantity
    );

    RETURN v_response;

EXCEPTION
    WHEN OTHERS THEN
        RETURN json_build_object('status', 'error', 'message', SQLERRM);
END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------

-- 3.2 Stock Transfer Function
-- Function untuk mentransfer stok antar gudang
CREATE OR REPLACE FUNCTION transfer_stock(
    p_product_id INT,
    p_from_warehouse_id INT,
    p_to_warehouse_id INT,
    p_quantity INT,
    p_notes TEXT DEFAULT NULL
) RETURNS JSON AS $$
DECLARE
    v_out_result JSON;
    v_in_result JSON;
BEGIN
    -- Validasi dasar
    IF p_quantity <= 0 THEN
        RETURN json_build_object('status', 'error', 'message', 'Quantity for transfer must be positive.');
    END IF;
    IF p_from_warehouse_id = p_to_warehouse_id THEN
        RETURN json_build_object('status', 'error', 'message', 'Cannot transfer stock to the same warehouse.');
    END IF;

    -- Gunakan transaksi untuk memastikan kedua pergerakan berhasil atau keduanya gagal
    BEGIN
        -- 1. Catat pergerakan 'OUT' dari gudang asal
        v_out_result := record_stock_movement(
            p_product_id,
            p_from_warehouse_id,
            'OUT',
            p_quantity,
            'TRANSFER',
            NULL, -- reference_id
            'Transfer OUT ke Gudang ' || p_to_warehouse_id || COALESCE(' - ' || p_notes, '')
        );

        IF (v_out_result ->> 'status') = 'error' THEN
            RAISE EXCEPTION '%', v_out_result ->> 'message';
        END IF;

        -- 2. Catat pergerakan 'IN' ke gudang tujuan
        -- Catatan: p_unit_cost di set NULL untuk transfer karena ini bukan pembelian baru.
        -- Perhitungan nilai stok yang akurat untuk transfer memerlukan mekanisme yang lebih kompleks
        -- untuk melacak biaya unit dari gudang asal.
        v_in_result := record_stock_movement(
            p_product_id,
            p_to_warehouse_id,
            'IN',
            p_quantity,
            'TRANSFER',
            NULL, -- reference_id
            'Transfer IN dari Gudang ' || p_from_warehouse_id || COALESCE(' - ' || p_notes, '')
        );

        IF (v_in_result ->> 'status') = 'error' THEN
            RAISE EXCEPTION '%', v_in_result ->> 'message';
        END IF;

        RETURN json_build_object(
            'status', 'success',
            'message', 'Stock transferred successfully.',
            'product_id', p_product_id,
            'from_warehouse_id', p_from_warehouse_id,
            'to_warehouse_id', p_to_warehouse_id,
            'quantity', p_quantity,
            'out_movement_id', v_out_result ->> 'movement_id',
            'in_movement_id', v_in_result ->> 'movement_id'
        );

    EXCEPTION
        WHEN OTHERS THEN
            RETURN json_build_object('status', 'error', 'message', 'Transfer failed: ' || SQLERRM);
    END;
END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------

-- 3.3 Reorder Alert Function
-- Function untuk memeriksa produk yang perlu dipesan ulang berdasarkan min_stock_level
CREATE OR REPLACE FUNCTION check_reorder_points(
    p_warehouse_id INT DEFAULT NULL
) RETURNS TABLE(
    product_id INT,
    product_name VARCHAR(255),
    warehouse_id INT,
    warehouse_name VARCHAR(255),
    current_stock INT,
    min_stock_level INT,
    reorder_status TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.product_id,
        p.name AS product_name,
        w.warehouse_id,
        w.name AS warehouse_name,
        COALESCE(s.quantity, 0) AS current_stock,
        rp.min_stock_level,
        CASE
            WHEN COALESCE(s.quantity, 0) <= rp.min_stock_level THEN 'BELOW_MIN'
            ELSE 'OK'
        END AS reorder_status
    FROM products p
    JOIN reorder_points rp ON p.product_id = rp.product_id
    LEFT JOIN stocks s ON p.product_id = s.product_id AND rp.warehouse_id = s.warehouse_id
    JOIN warehouses w ON rp.warehouse_id = w.warehouse_id
    WHERE (p_warehouse_id IS NULL OR rp.warehouse_id = p_warehouse_id)
    ORDER BY p.name, w.name;
END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------

-- 3.4 Stock Valuation Function
-- Function untuk menghitung nilai stok (FIFO/LIFO/Average)
CREATE OR REPLACE FUNCTION calculate_stock_value(
    p_method VARCHAR(10), -- 'FIFO', 'LIFO', 'AVG'
    p_warehouse_id INT DEFAULT NULL,
    p_product_id INT DEFAULT NULL
) RETURNS TABLE(
    product_id INT,
    product_name VARCHAR(255),
    warehouse_id INT,
    warehouse_name VARCHAR(255),
    current_stock INT,
    total_value DECIMAL(18, 2)
) AS $$
DECLARE
    v_product_id INT;
    v_warehouse_id INT;
    v_current_stock_qty INT;
    v_total_value DECIMAL(18, 2);
    r RECORD;
BEGIN
    -- Temporary table to store results
    CREATE TEMPORARY TABLE IF NOT EXISTS temp_stock_valuation (
        product_id INT,
        product_name VARCHAR(255),
        warehouse_id INT,
        warehouse_name VARCHAR(255),
        current_stock INT,
        total_value DECIMAL(18, 2)
    ) ON COMMIT DROP; -- Drop table after transaction

    -- Loop melalui setiap produk di setiap gudang yang relevan
    FOR r IN
        SELECT s.product_id, s.warehouse_id, s.quantity
        FROM stocks s
        WHERE (p_warehouse_id IS NULL OR s.warehouse_id = p_warehouse_id)
          AND (p_product_id IS NULL OR s.product_id = p_product_id)
        ORDER BY s.product_id, s.warehouse_id
    LOOP
        v_product_id := r.product_id;
        v_warehouse_id := r.warehouse_id;
        v_current_stock_qty := r.quantity; -- Quantity aktual di tabel stocks
        v_total_value := 0.00;

        IF v_current_stock_qty = 0 THEN
            v_total_value := 0.00;
        ELSIF p_method = 'FIFO' THEN
            -- FIFO: Ambil dari penerimaan terlama yang masih memiliki sisa
            FOR receipt IN
                SELECT sr.unit_cost, sr.remaining_quantity
                FROM stock_receipt_costs sr
                WHERE sr.product_id = v_product_id AND sr.warehouse_id = v_warehouse_id AND sr.remaining_quantity > 0
                ORDER BY sr.receipt_date ASC, sr.receipt_id ASC
            LOOP
                IF v_current_stock_qty <= 0 THEN
                    EXIT;
                END IF;

                DECLARE
                    qty_to_use INT := LEAST(v_current_stock_qty, receipt.remaining_quantity);
                BEGIN
                    v_total_value := v_total_value + (qty_to_use * receipt.unit_cost);
                    v_current_stock_qty := v_current_stock_qty - qty_to_use;
                END;
            END LOOP;

        ELSIF p_method = 'LIFO' THEN
            -- LIFO: Ambil dari penerimaan terbaru yang masih memiliki sisa
            FOR receipt IN
                SELECT sr.unit_cost, sr.remaining_quantity
                FROM stock_receipt_costs sr
                WHERE sr.product_id = v_product_id AND sr.warehouse_id = v_warehouse_id AND sr.remaining_quantity > 0
                ORDER BY sr.receipt_date DESC, sr.receipt_id DESC
            LOOP
                IF v_current_stock_qty <= 0 THEN
                    EXIT;
                END IF;

                DECLARE
                    qty_to_use INT := LEAST(v_current_stock_qty, receipt.remaining_quantity);
                BEGIN
                    v_total_value := v_total_value + (qty_to_use * receipt.unit_cost);
                    v_current_stock_qty := v_current_stock_qty - qty_to_use;
                END;
            END LOOP;

        ELSIF p_method = 'AVG' THEN
            -- Weighted Average Cost: Hitung rata-rata tertimbang dari semua penerimaan yang belum terjual
            DECLARE
                total_receipt_qty_remaining INT := 0;
                total_receipt_cost_remaining DECIMAL(18, 2) := 0.00;
                avg_unit_cost DECIMAL(10, 2) := 0.00;
            BEGIN
                SELECT SUM(sr.remaining_quantity), SUM(sr.remaining_quantity * sr.unit_cost)
                INTO total_receipt_qty_remaining, total_receipt_cost_remaining
                FROM stock_receipt_costs sr
                WHERE sr.product_id = v_product_id AND sr.warehouse_id = v_warehouse_id AND sr.remaining_quantity > 0;

                IF total_receipt_qty_remaining > 0 THEN
                    avg_unit_cost := total_receipt_cost_remaining / total_receipt_qty_remaining;
                    -- Gunakan stok aktual yang ada di tabel stocks
                    v_total_value := avg_unit_cost * v_current_stock_qty;
                ELSE
                    v_total_value := 0.00;
                END IF;
            END;

        ELSE
            RAISE EXCEPTION 'Invalid valuation method. Choose FIFO, LIFO, or AVG.';
        END IF;

        -- Insert results into temporary table
        INSERT INTO temp_stock_valuation
        SELECT
            v_product_id,
            p.name,
            v_warehouse_id,
            w.name,
            r.quantity, -- Stok aktual dari loop awal
            v_total_value
        FROM products p, warehouses w
        WHERE p.product_id = v_product_id AND w.warehouse_id = v_warehouse_id;

    END LOOP;

    RETURN QUERY SELECT * FROM temp_stock_valuation;
END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------

-- 3.5 Audit Trigger
-- Fungsi trigger untuk mencatat perubahan pada tabel stocks ke audit_log
CREATE OR REPLACE FUNCTION audit_stock_changes_func()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO audit_log (table_name, record_id, operation_type, new_data, changed_by)
        VALUES (TG_TABLE_NAME, NEW.stock_id, 'INSERT', to_jsonb(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO audit_log (table_name, record_id, operation_type, old_data, new_data, changed_by)
        VALUES (TG_TABLE_NAME, OLD.stock_id, 'UPDATE', to_jsonb(OLD), to_jsonb(NEW), CURRENT_USER);
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO audit_log (table_name, record_id, operation_type, old_data, changed_by)
        VALUES (TG_TABLE_NAME, OLD.stock_id, 'DELETE', to_jsonb(OLD), CURRENT_USER);
        RETURN OLD;
    END IF;
    RETURN NULL; -- Harusnya tidak tercapai
END;
$$ LANGUAGE plpgsql;

-- Trigger pada tabel stocks
-- Trigger ini akan aktif setiap kali ada INSERT, UPDATE, atau DELETE pada tabel stocks.
CREATE TRIGGER audit_stock_changes
AFTER INSERT OR UPDATE OR DELETE ON stocks
FOR EACH ROW
EXECUTE FUNCTION audit_stock_changes_func();

-- --------------------------------------------------------
-- END OF SCRIPT
-- --------------------------------------------------------