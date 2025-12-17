import json
import os
from datetime import datetime
from collections import defaultdict

def parse_to_structure(input_file=None, output_file=None):
    """
    Преобразует raw данные парсера в структурированные метрики
    Входной формат: {дата: {район: [запросы]}}
    Выходной формат: {дата: {район: {метрики}}}
    """
    env_output_file = os.environ.get("TRANSFORM_OUTPUT_FILE")
    if env_output_file:
        output_file = env_output_file
    
    if input_file is None or output_file is None:
        config_file = os.path.join(os.getcwd(), "config.json")
        if os.path.exists(config_file):
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
            input_file = input_file or config["output_raw_file"]
            output_file = output_file or config["output_final_file"]
        else:
            input_file = input_file or "metrics_7days_raw.json"
            output_file = output_file or "metrics_7days.json"
    
    input_file = os.path.join(os.getcwd(), input_file)
    output_file = os.path.join(os.getcwd(), output_file)
    
    if not os.path.exists(input_file):
        print(f"[ERROR] Файл {input_file} не найден")
        return None
    
    print(f"[OK] Загружаю: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    
    result = {}
    
    for date_key, areas_data in raw_data.items():
        print(f'\n{"="*80}')
        print(f'Обработка даты: {date_key}')
        print(f'{"="*80}')
        
        if date_key not in result:
            result[date_key] = {}
        
        for area_name, requests in areas_data.items():
            print(f'\nОбработка района: {area_name}')
            print('-'*80)
            
            area_data = {
                'sales_volume': {'off_plan_properties': None, 'ready_properties': None},
                'sales_avg_price': {'off_plan_properties': None, 'ready_properties': None},
                'sales_listing_volume': None,
                'sales_listing_avg_price': None,
                'rent_volume': {'new_rentals': None, 'renewed_rentals': None},
                'rent_listing_volume': None,
                'rent_listing_avg_price': None,
                'rent_avg_price': {'new_rentals': None, 'renewed_rentals': None},
            }
            
            for req_idx, req in enumerate(requests, 1):
                request = req['request']
                response = req['response']
                
                queries = request.get('queries', [])
                if not queries:
                    continue
                
                cmd = queries[0]['Query']['Commands'][0]
                sq = cmd['SemanticQueryDataShapeCommand']['Query']
                wheres = sq.get('Where', [])
                
                where_dict = {}
                
                for where in wheres:
                    cond = where.get('Condition', {})
                    if 'In' in cond:
                        expr = cond['In']['Expressions'][0]
                        prop = expr.get('Column', {}).get('Property', '')
                        vals = cond['In'].get('Values', [])
                        
                        clean_vals = []
                        for val_group in vals:
                            for val_item in val_group:
                                if 'Literal' in val_item:
                                    clean_vals.append(val_item['Literal']['Value'].strip("'"))
                        where_dict[prop] = clean_vals
                    elif 'Comparison' in cond:
                        comp = cond['Comparison']
                        prop = comp.get('Left', {}).get('Column', {}).get('Property', '')
                        val = comp.get('Right', {}).get('Literal', {}).get('Value', '')
                        where_dict[prop] = val
                
                results_list = response.get('results', [])
                if not results_list:
                    continue
                
                for result_idx, result_item in enumerate(results_list):
                    job_id = result_item.get('jobId', '0')
                    query_idx = int(job_id) if str(job_id).isdigit() else 0
                    
                    print(f'    [PROCESSING RES #{result_idx}] jobId={job_id}, query_idx={query_idx}')
                    
                    current_where_dict = where_dict.copy()
                    if query_idx < len(queries):
                        current_query = queries[query_idx]
                        query_cmd = current_query['Query']['Commands'][0]
                        query_sq = query_cmd['SemanticQueryDataShapeCommand']['Query']
                        
                        selects = query_sq.get('Select', [])
                        select_names = [s.get('Name', '') for s in selects]
                        
                        query_wheres = query_sq.get('Where', [])
                        
                        for where in query_wheres:
                            cond = where.get('Condition', {})
                            if 'In' in cond:
                                expr = cond['In']['Expressions'][0]
                                prop = expr.get('Column', {}).get('Property', '')
                                vals = cond['In'].get('Values', [])
                                
                                clean_vals = []
                                for val_group in vals:
                                    for val_item in val_group:
                                        if 'Literal' in val_item:
                                            clean_vals.append(val_item['Literal']['Value'].strip("'"))
                                current_where_dict[prop] = clean_vals
                    else:
                        cmd = queries[0]['Query']['Commands'][0]
                        sq = cmd['SemanticQueryDataShapeCommand']['Query']
                        selects = sq.get('Select', [])
                        select_names = [s.get('Name', '') for s in selects]
                    
                    print(f'    [SELECT для RES #{result_idx}] {select_names[:1]}')
                    
                    result_data = result_item.get('result', {}).get('data', {})
                    dsr = result_data.get('dsr', {})
                    ds_list = dsr.get('DS', [])
                
                    if not ds_list:
                        print(f'    [SKIP RES #{result_idx}] ds_list пустой')
                        continue
                
                    ds = ds_list[0]
                    ph_list = ds.get('PH', [])
                
                    if not ph_list:
                        print(f'    [SKIP RES #{result_idx}] ph_list пустой')
                        continue
                
                    ph = ph_list[0]
                
                    for dm_key in ph:
                        if not dm_key.startswith('DM'):
                            continue
                    
                        dm_array = ph[dm_key]
                    
                        if '##Transaction Volume' in select_names[0]:
                            transaction_type = current_where_dict.get('Transaction Type', [''])[0]
                            version = current_where_dict.get('Version', [''])[0] if 'Version' in current_where_dict else None
                            job_id = result_item.get('jobId', '0')
                        
                            if transaction_type == 'Rent':
                                print(f'  [REQ #{req_idx}, RES #{result_idx}] TRX VOL Rent: version={version}, jobId={job_id}')
                        
                            value = None
                            for dm in dm_array:
                                for k, v in dm.items():
                                    if k.startswith('M') or isinstance(v, (int, float)):
                                        value = v
                                        break
                        
                            if value is not None:
                            
                                if transaction_type == 'Sales - Ready':
                                    area_data['sales_volume']['ready_properties'] = float(value)
                                    print(f'  [{req_idx}] sales_volume.ready_properties = {value}')
                                elif transaction_type == 'Sales - Off-Plan':
                                    area_data['sales_volume']['off_plan_properties'] = float(value)
                                    print(f'  [{req_idx}] sales_volume.off_plan_properties = {value}')
                                elif transaction_type == 'Rent':
                                    if version == 'New':
                                        area_data['rent_volume']['new_rentals'] = float(value)
                                        print(f'  [{req_idx}] rent_volume.new_rentals = {value}')
                                    elif version == 'Renewed':
                                        area_data['rent_volume']['renewed_rentals'] = float(value)
                                        print(f'  [{req_idx}] rent_volume.renewed_rentals = {value}')
                    
                        elif '##Transaction Avg Price' in select_names[0]:
                            transaction_type = current_where_dict.get('Transaction Type', [''])[0]
                            version = current_where_dict.get('Version', [''])[0] if 'Version' in current_where_dict else None
                            job_id = result_item.get('jobId', '0')
                        
                            value = None
                            for dm in dm_array:
                                for k, v in dm.items():
                                    if k.startswith('M') or isinstance(v, (int, float)):
                                        value = v
                                        break
                        
                            if value is not None:
                                if transaction_type == 'Sales - Ready':
                                    area_data['sales_avg_price']['ready_properties'] = float(value)
                                    print(f'  [{req_idx}] sales_avg_price.ready_properties = {value}')
                                elif transaction_type == 'Sales - Off-Plan':
                                    area_data['sales_avg_price']['off_plan_properties'] = float(value)
                                    print(f'  [{req_idx}] sales_avg_price.off_plan_properties = {value}')
                                elif transaction_type == 'Rent':
                                    if version == 'New':
                                        area_data['rent_avg_price']['new_rentals'] = float(value)
                                        print(f'  [{req_idx}] rent_avg_price.new_rentals = {value} (version={version}, jobId={job_id})')
                                    elif version == 'Renewed':
                                        area_data['rent_avg_price']['renewed_rentals'] = float(value)
                                        print(f'  [{req_idx}] rent_avg_price.renewed_rentals = {value} (version={version}, jobId={job_id})')
                    
                        elif '#Listing Volume' in select_names[0]:
                            listing_type = current_where_dict.get('Listing Type', [''])[0]
                        
                            value = None
                            for dm in dm_array:
                                for k, v in dm.items():
                                    if k.startswith('M') or isinstance(v, (int, float)):
                                        value = v
                                        break
                        
                            if value is not None:
                                if listing_type == 'Sale':
                                    area_data['sales_listing_volume'] = float(value)
                                    print(f'  [{req_idx}] sales_listing_volume = {value}')
                                elif listing_type == 'Rent':
                                    area_data['rent_listing_volume'] = float(value)
                                    print(f'  [{req_idx}] rent_listing_volume = {value}')
                    
                        elif '#Listing Avg Price' in select_names[0]:
                            listing_type = current_where_dict.get('Listing Type', [''])[0]
                        
                            value = None
                            for dm in dm_array:
                                for k, v in dm.items():
                                    if k.startswith('M') or isinstance(v, (int, float)):
                                        value = v
                                        break
                        
                            if value is not None:
                                if listing_type == 'Sale':
                                    area_data['sales_listing_avg_price'] = float(value)
                                    print(f'  [{req_idx}] sales_listing_avg_price = {value}')
                                elif listing_type == 'Rent':
                                    area_data['rent_listing_avg_price'] = float(value)
                                    print(f'  [{req_idx}] rent_listing_avg_price = {value}')
                    
                        elif 'Avg(pbi_ae_indicators_mv.Value)' in select_names[0]:
                            data_type = current_where_dict.get('Data Type', [''])[0]
                        
                            if dm_array and 'G0' in dm_array[0]:
                                sh_data = ds.get('SH', [])
                                bedroom_labels = []
                                if sh_data:
                                    dm1_data = sh_data[0].get('DM1', [])
                                    for item in dm1_data:
                                        label = item.get('G1', '')
                                        bedroom_labels.append(label)
                            
                                monthly_data_by_bedroom = {}
                            
                                for dm in dm_array:
                                    if 'G0' in dm and 'X' in dm:
                                        timestamp = dm['G0']
                                        if timestamp > 1000000000000:
                                            date_obj = datetime.fromtimestamp(timestamp / 1000)
                                            month_key = date_obj.strftime('%Y-%m')
                                        
                                            x_array = dm['X']
                                            bedroom_values = {}
                                        
                                            for idx, x_item in enumerate(x_array):
                                                value = None
                                                for m_key, m_val in x_item.items():
                                                    if m_key.startswith('M'):
                                                        value = float(m_val) if isinstance(m_val, (int, float, str)) else m_val
                                                        break
                                            
                                                if bedroom_labels and idx < len(bedroom_labels):
                                                    bedroom_key = bedroom_labels[idx]
                                                else:
                                                    bedroom_key = str(idx)
                                            
                                                if value is not None:
                                                    bedroom_values[bedroom_key] = value
                                        
                                            if bedroom_values:
                                                monthly_data_by_bedroom[month_key] = bedroom_values
                            
                                if monthly_data_by_bedroom:
                                    if data_type == 'Sales Prices':
                                        area_data['sales_price_trend'] = monthly_data_by_bedroom
                                        print(f'  [{req_idx}] sales_price_trend: {len(monthly_data_by_bedroom)} месяцев')
                                    elif data_type == 'Rent Values':
                                        area_data['rent_price_trend'] = monthly_data_by_bedroom
                                        print(f'  [{req_idx}] rent_price_trend: {len(monthly_data_by_bedroom)} месяцев')
                                    elif data_type == 'Yield Rates':
                                        area_data['gross_rental_yield'] = monthly_data_by_bedroom
                                        print(f'  [{req_idx}] gross_rental_yield: {len(monthly_data_by_bedroom)} месяцев')
                                    elif data_type == 'Price-to-Rent Ratios':
                                        area_data['price_to_rent_ratio'] = monthly_data_by_bedroom
                                        print(f'  [{req_idx}] price_to_rent_ratio: {len(monthly_data_by_bedroom)} месяцев')
                    
                        elif 'Sum(pbi_ae_indicators_mv.value)' in select_names[0] and 'Calendar.Year' in select_names:
                            data_type = current_where_dict.get('Data Type', [''])[0]
                        
                            if data_type in ['Occupancy Rate', 'Service Charges'] and dm_array and 'G0' in dm_array[0]:
                                sh_data = ds.get('SH', [])
                                property_labels = []
                                if sh_data:
                                    dm1_data = sh_data[0].get('DM1', [])
                                    for item in dm1_data:
                                        label = item.get('G1', '')
                                        property_labels.append(label)
                            
                                yearly_data_by_property = {}
                            
                                for dm in dm_array:
                                    if 'G0' in dm and 'X' in dm:
                                        year = str(dm['G0'])
                                        x_array = dm['X']
                                        property_values = {}
                                    
                                        for idx, x_item in enumerate(x_array):
                                            value = None
                                            for m_key, m_val in x_item.items():
                                                if m_key.startswith('M'):
                                                    value = float(m_val) if isinstance(m_val, (int, float, str)) else m_val
                                                    break
                                        
                                            if property_labels and idx < len(property_labels):
                                                property_key = property_labels[idx]
                                            else:
                                                property_key = str(idx)
                                        
                                            if value is not None:
                                                property_values[property_key] = value
                                    
                                        if property_values:
                                            yearly_data_by_property[year] = property_values
                            
                                if yearly_data_by_property:
                                    if data_type == 'Occupancy Rate':
                                        area_data['occupancy_rate'] = yearly_data_by_property
                                        print(f'  [{req_idx}] occupancy_rate: {len(yearly_data_by_property)} лет')
                                    elif data_type == 'Service Charges':
                                        area_data['average_service_charges'] = yearly_data_by_property
                                        print(f'  [{req_idx}] average_service_charges: {len(yearly_data_by_property)} лет')
                    
                        elif 'Sum(pbi_ae_supply_mv.number_of_unit)' in select_names[0]:
                            status = current_where_dict.get('Status', [''])[0] if isinstance(current_where_dict.get('Status', ['']), list) else current_where_dict.get('Status', '')
                        
                            has_status_grouping = 'pbi_ae_supply_mv.property_status' in select_names
                        
                            if has_status_grouping and dm_array and 'C' in dm_array[0] and 'G0' not in dm_array[0]:
                                supply_by_status = {}
                            
                                for dm in dm_array:
                                    if 'C' in dm:
                                        cat_values = dm['C']
                                        if len(cat_values) >= 2:
                                            status_name = cat_values[0]
                                            status_value = cat_values[1]
                                            supply_by_status[status_name] = status_value
                            
                                if supply_by_status:
                                    area_data['residential_supply'] = supply_by_status
                                    print(f'  [{req_idx}] residential_supply: {len(supply_by_status)} статусов')
                        
                            elif dm_array and 'C' in dm_array[0] and 'G0' not in dm_array[0]:
                                categories_data = {}
                            
                                value_dicts = ds.get('ValueDicts', {})
                                bedroom_labels = value_dicts.get('D0', [])
                            
                                for dm in dm_array:
                                    if 'C' in dm:
                                        cat_values = dm['C']
                                        if len(cat_values) >= 2:
                                            category_index = cat_values[0]
                                            category_value = cat_values[1]
                                        
                                            if bedroom_labels and category_index < len(bedroom_labels):
                                                category_key = bedroom_labels[category_index]
                                            else:
                                                category_key = str(category_index)
                                        
                                            categories_data[category_key] = category_value
                            
                                if categories_data:
                                    if status == 'Existing':
                                        area_data['ready_supply_by_bedroom'] = categories_data
                                        print(f'  [{req_idx}] ready_supply_by_bedroom: {len(categories_data)} категорий')
                                    elif status == 'Under Construction':
                                        area_data['upcoming_supply_by_bedroom'] = categories_data
                                        print(f'  [{req_idx}] upcoming_supply_by_bedroom: {len(categories_data)} категорий')
                    
                        elif 'Sum(pbi_ae_supply_mv.Units)' in select_names and 'pbi_ae_supply_mv.property_status' in select_names:
                            if dm_array and 'G0' in dm_array[0]:
                                sh_data = ds.get('SH', [])
                                status_labels = []
                                if sh_data:
                                    dm1_data = sh_data[0].get('DM1', [])
                                    for item in dm1_data:
                                        label = item.get('G1', '')
                                        status_labels.append(label)
                            
                                yearly_supply_by_status = {}
                            
                                for dm in dm_array:
                                    if 'G0' in dm and 'X' in dm:
                                        year = str(dm['G0'])
                                        x_array = dm['X']
                                        status_values = {}
                                    
                                        for idx, x_item in enumerate(x_array):
                                            value = None
                                            status_idx = x_item.get('I', idx)
                                        
                                            for m_key, m_val in x_item.items():
                                                if m_key.startswith('M'):
                                                    value = int(m_val) if isinstance(m_val, (int, float, str)) else m_val
                                                    break
                                        
                                            if status_labels and status_idx < len(status_labels):
                                                status_key = status_labels[status_idx]
                                            else:
                                                status_key = str(status_idx)
                                        
                                            if value is not None:
                                                status_values[status_key] = value
                                    
                                        if status_values:
                                            yearly_supply_by_status[year] = status_values
                            
                                if yearly_supply_by_status:
                                    area_data['residential_supply_trend_by_year'] = yearly_supply_by_status
                                    print(f'  [{req_idx}] residential_supply_trend_by_year: {len(yearly_supply_by_status)} лет')
            
                result[date_key][area_name] = area_data
            print(f'\n[OK] {area_name} завершен')
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    total_dates = len(result)
    total_areas = sum(len(areas) for areas in result.values())
    
    print(f'\n{"="*80}')
    print(f'[OK] {output_file} создан')
    print(f'[OK] Дат: {total_dates}, Районов всего: {total_areas}')
    print(f'{"="*80}\n')
    
    return result

if __name__ == '__main__':
    parse_to_structure()
