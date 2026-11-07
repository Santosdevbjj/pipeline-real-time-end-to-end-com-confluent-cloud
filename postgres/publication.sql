-- Execute as a database owner after creating the CDC role.
CREATE PUBLICATION payments_cdc FOR TABLE public.accounts, public.transactions;
GRANT CONNECT ON DATABASE payments TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON public.accounts, public.transactions TO cdc_user;
