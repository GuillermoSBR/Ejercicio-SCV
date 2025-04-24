def get_aqi_categories_query():
    return """
        SELECT * from public.aqi_categorias
    """