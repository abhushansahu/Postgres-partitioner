DROP TRIGGER IF EXISTS insert_{0} on public.{0};
CREATE TRIGGER insert_{0}
    BEFORE INSERT ON public.{0}
    FOR EACH ROW EXECUTE PROCEDURE public.function_{0}();
