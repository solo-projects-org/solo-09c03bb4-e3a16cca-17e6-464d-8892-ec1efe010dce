-- 水质样品表：样品号、站号、pH
CREATE TABLE IF NOT EXISTS public.samples (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sample_no   text NOT NULL UNIQUE,                 -- 样品号
    station_no  text NOT NULL,                        -- 站号
    ph          numeric(4,2) NOT NULL CHECK (ph BETWEEN 0 AND 14),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_samples_station ON public.samples (station_no);

-- 预置样品数据
INSERT INTO public.samples (sample_no, station_no, ph) VALUES
    ('S-2026-0001', 'ST-01', 7.10),
    ('S-2026-0002', 'ST-01', 6.95),
    ('S-2026-0003', 'ST-02', 7.40);
