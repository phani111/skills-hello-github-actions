yyyymm_range_list = [
    f"{year}{month:02d}"
    for year in range(2022, 2025)
    for month in range(1, 13)
    if not (year == 2022 and month < 7) and not (year == 2024 and month > 7)
]
