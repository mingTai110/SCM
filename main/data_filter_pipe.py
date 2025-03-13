def process_and_filter_data(preprocessor, ABC_class, MSU, PG_class, start_date, end_date, used_data, open_data_list):
    # Filter historical data
    filtered_data = preprocessor.filter_data(
        data=used_data,
        MSU=MSU,
        ABC_class=ABC_class,
        PG_class=PG_class,
        start_date=start_date,
        end_date=end_date
    )

    # Filter open_data
    filtered_open_data = {
        gap_year + 1: preprocessor.filter_data(
            data=open_data,
            MSU=MSU,
            ABC_class=ABC_class,
            PG_class=PG_class,
            start_date=start_date,
            end_date=end_date
        )
        for gap_year, open_data in enumerate(open_data_list)
    }

    return filtered_data, filtered_open_data