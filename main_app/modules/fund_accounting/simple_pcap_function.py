def simple_pcap_function(pcap_data, pcap_summary, input):
    """Simple PCAP generation function"""
    try:
        # Get inputs
        as_of_date = input.pcap_as_of_date()
        currency = input.pcap_currency()
        selected_fund_id = input.pcap_fund_select() if hasattr(input, 'pcap_fund_select') else "fund_i_class_B_ETH"
        
        print(f"PCAP Generation - Fund: {selected_fund_id}, Currency: {currency}, As of: {as_of_date}")
        
        # Load GL and COA data
        from ...s3_utils import load_GL_file, load_COA_file, load_GP_incentive_audit_file, load_LP_commitments_file
        import pandas as pd
        
        gl_df = load_GL_file()
        coa_df = load_COA_file()
        
        if gl_df is None or gl_df.empty:
            pcap_summary.set({"error": "No GL data available", "as_of_date": as_of_date})
            return
            
        if coa_df is None or coa_df.empty:
            pcap_summary.set({"error": "No COA data available", "as_of_date": as_of_date})
            return
        
        print(f"Loaded GL data: {len(gl_df)} records")
        print(f"Loaded COA data: {len(coa_df)} accounts")
        
        # Filter GL data by date and fund
        # Convert to UTC timezone-aware to match GL data
        as_of_datetime = pd.to_datetime(as_of_date).tz_localize('UTC')
        gl_filtered = gl_df[
            (gl_df['date'] <= as_of_datetime) & 
            (gl_df['fund_id'] == selected_fund_id)
        ].copy()
        
        print(f"Filtered GL data: {len(gl_filtered)} records")
        
        # Join GL data with COA to get SCPC categories
        gl_with_scpc = gl_filtered.merge(
            coa_df[['account_name', 'SCPC']].dropna(), 
            on='account_name', 
            how='left'
        )
        
        print(f"GL data after COA join: {len(gl_with_scpc)} records")
        
        # Get all SCPC categories from COA ordered by schedule_ranking
        scpc_mapping = coa_df[['SCPC', 'schedule_ranking']].dropna().drop_duplicates()
        scpc_ordered = scpc_mapping.sort_values('schedule_ranking')
        print(f"Found {len(scpc_ordered)} SCPC categories from COA:")
        print(scpc_ordered.to_string(index=False))
        
        # Calculate periods (use the already converted timezone-aware datetime)
        as_of_dt = as_of_datetime
        
        # Month to date
        mtd_start = as_of_dt.replace(day=1)
        
        # Quarter to date  
        quarter = (as_of_dt.month - 1) // 3 + 1
        qtd_start = as_of_dt.replace(month=(quarter-1)*3 + 1, day=1)
        
        # Year to date
        ytd_start = as_of_dt.replace(month=1, day=1)
        
        # Inception to date (earliest GL date)
        itd_start = gl_with_scpc['date'].min() if not gl_with_scpc.empty else as_of_dt
        
        print(f"Period calculations:")
        print(f"  MTD: {mtd_start.date()} to {as_of_dt.date()}")
        print(f"  QTD: {qtd_start.date()} to {as_of_dt.date()}")
        print(f"  YTD: {ytd_start.date()} to {as_of_dt.date()}")
        print(f"  ITD: {itd_start.date()} to {as_of_dt.date()}")
        
        # Create PCAP table with all SCPC categories in schedule_ranking order
        pcap_rows = []
        
        # First, calculate total beginning balance (all transactions before current month)
        gl_with_scpc['credit_crypto_float'] = pd.to_numeric(gl_with_scpc['credit_crypto'], errors='coerce').fillna(0.0)
        gl_with_scpc['debit_crypto_float'] = pd.to_numeric(gl_with_scpc['debit_crypto'], errors='coerce').fillna(0.0)
        gl_with_scpc['net_amount'] = gl_with_scpc['credit_crypto_float'] - gl_with_scpc['debit_crypto_float']
        
        total_beginning_balance = gl_with_scpc[gl_with_scpc['date'] < mtd_start]['net_amount'].sum()
        
        # Add Beginning Balance row
        pcap_rows.append({
            'Line_Item': 'Beginning Balance',
            'MTD': 0.0,
            'QTD': 0.0,
            'YTD': 0.0,
            'ITD': 0.0
        })
        
        # Add all SCPC line items in schedule_ranking order
        for _, row in scpc_ordered.iterrows():
            scpc = row['SCPC']
            ranking = row['schedule_ranking']
            
            # Filter GL data for this SCPC category
            scpc_data = gl_with_scpc[gl_with_scpc['SCPC'] == scpc].copy()
            
            if scpc_data.empty:
                # Show zero values for SCPC categories with no transactions
                mtd_total = qtd_total = ytd_total = itd_total = 0.0
            else:
                # Calculate period totals
                mtd_total = scpc_data[scpc_data['date'] >= mtd_start]['net_amount'].sum()
                qtd_total = scpc_data[scpc_data['date'] >= qtd_start]['net_amount'].sum()
                ytd_total = scpc_data[scpc_data['date'] >= ytd_start]['net_amount'].sum()
                itd_total = scpc_data[scpc_data['date'] >= itd_start]['net_amount'].sum()
                
                # No longer need beginning/ending balance calculations
            
            pcap_rows.append({
                'Line_Item': f"{scpc}",
                'MTD': float(mtd_total),
                'QTD': float(qtd_total), 
                'YTD': float(ytd_total),
                'ITD': float(itd_total)
            })
            
            print(f"  {ranking} - {scpc}: MTD={mtd_total:.4f}, QTD={qtd_total:.4f}, YTD={ytd_total:.4f}, ITD={itd_total:.4f}")
        
        # Calculate ending capital as sum of all line items above
        # Sum all the SCPC line items for each column
        total_mtd = sum(row['MTD'] for row in pcap_rows[1:])  # Skip beginning balance row
        total_qtd = sum(row['QTD'] for row in pcap_rows[1:])
        total_ytd = sum(row['YTD'] for row in pcap_rows[1:])
        total_itd = sum(row['ITD'] for row in pcap_rows[1:])
        
        # Add Ending Capital row as sum of all line items
        pcap_rows.append({
            'Line_Item': 'Ending Capital',
            'MTD': float(total_mtd),
            'QTD': float(total_qtd),
            'YTD': float(total_ytd),
            'ITD': float(total_itd)
        })
        
        # Create DataFrame
        pcap_df = pd.DataFrame(pcap_rows)
        
        # Calculate performance metrics
        performance_metrics = {}
        
        if not pcap_df.empty:
            # Calculate Gross MOIC from PCAP data
            try:
                # Find Capital contributions (SCPC_1)
                contrib_rows = pcap_df[pcap_df['Line_Item'] == 'Capital contributions']
                total_contributions = float(contrib_rows['ITD'].iloc[0]) if not contrib_rows.empty else 0.0
                
                # Find Capital distributions (SCPC_2) 
                dist_rows = pcap_df[pcap_df['Line_Item'] == 'Capital distributions']
                total_distributions = abs(float(dist_rows['ITD'].iloc[0])) if not dist_rows.empty else 0.0
                
                # Find Ending Capital
                ending_rows = pcap_df[pcap_df['Line_Item'] == 'Ending Capital']
                current_nav = float(ending_rows['ITD'].iloc[0]) if not ending_rows.empty else 0.0
                
                # Calculate Gross MOIC: (Distributions + Current NAV) / Contributions
                if total_contributions > 0:
                    gross_moic = (total_distributions + current_nav) / total_contributions
                    gross_moic_str = f"{gross_moic:.6f}"
                else:
                    gross_moic_str = "N/A"
                    
                print(f"Performance Metrics:")
                print(f"  - Total Contributions: {total_contributions:.6f}")
                print(f"  - Total Distributions: {total_distributions:.6f}")
                print(f"  - Current NAV: {current_nav:.6f}")
                print(f"  - Gross MOIC: {gross_moic_str}")
                
            except Exception as e:
                print(f"Error calculating Gross MOIC: {e}")
                gross_moic_str = "N/A"
            
            # Get Capital Committed and Capital Called from commitments data
            try:
                commitments_df = load_LP_commitments_file()
                if not commitments_df.empty and 'fund_id' in commitments_df.columns and 'commitment_amount' in commitments_df.columns:
                    # Filter commitments for the selected fund
                    fund_commitments = commitments_df[commitments_df['fund_id'] == selected_fund_id]
                    
                    if len(fund_commitments) > 0:
                        # Sum all LP commitments for this fund
                        capital_committed = float(fund_commitments['commitment_amount'].sum())
                        
                        # Capital Called = Total Contributions (what's been called/contributed so far)
                        capital_called = total_contributions
                        
                        print(f"  - Capital Committed: {capital_committed:.6f}")
                        print(f"  - Capital Called: {capital_called:.6f}")
                        
                        capital_committed_str = f"{capital_committed:.6f}"
                        capital_called_str = f"{capital_called:.6f}"
                    else:
                        print(f"  - No commitment data found for fund {selected_fund_id}")
                        capital_committed_str = "N/A"
                        capital_called_str = "N/A"
                else:
                    print("  - Commitments file not available")
                    capital_committed_str = "N/A"
                    capital_called_str = "N/A"
            except Exception as e:
                print(f"Error getting commitment data: {e}")
                capital_committed_str = "N/A"
                capital_called_str = "N/A"
            
            # Get Net IRR from audit trail - filter by selected fund
            try:
                audit_df = load_GP_incentive_audit_file()
                if not audit_df.empty and 'limited_partner_ID' in audit_df.columns and 'LP_net_irr_annualized' in audit_df.columns and 'fund_id' in audit_df.columns:
                    # Filter audit data to only LPs for the selected fund
                    fund_lps = audit_df[audit_df['fund_id'] == selected_fund_id]
                    print(f"  - Found {len(fund_lps)} LPs for fund {selected_fund_id}")
                    
                    if len(fund_lps) > 0:
                        # Use the first LP for this fund (or could be made configurable)
                        net_irr_value = fund_lps['LP_net_irr_annualized'].iloc[0]
                        lp_id = fund_lps['limited_partner_ID'].iloc[0]
                        print(f"  - Using LP: {lp_id}")
                        
                        if pd.isna(net_irr_value) or net_irr_value == 0:
                            net_irr = "N/A"
                        else:
                            # Format as percentage if it's a decimal (between -1 and 1), otherwise use as-is
                            if isinstance(net_irr_value, (int, float)) and abs(net_irr_value) < 1:
                                net_irr = f"{float(net_irr_value) * 100:.2f}%"
                            else:
                                net_irr = str(net_irr_value)
                    else:
                        net_irr = "N/A"
                        print(f"  - No LPs found for fund {selected_fund_id}")
                else:
                    net_irr = "N/A"
                print(f"  - Net IRR: {net_irr}")
            except Exception as e:
                print(f"Error getting Net IRR: {e}")
                net_irr = "N/A"
            
            performance_metrics = {
                "net_irr": net_irr,
                "gross_moic": gross_moic_str,
                "capital_committed": capital_committed_str,
                "capital_called": capital_called_str,
                "currency": currency,
                "fund_name": selected_fund_id
            }
        
        if not pcap_df.empty:
            print(f"Simple PCAP generated successfully! {len(pcap_df)} line items")
            pcap_data.set(pcap_df)
            pcap_summary.set({
                "success": True,
                "message": f"PCAP generated: {len(pcap_df)} line items",
                "as_of_date": as_of_date,
                "fund": selected_fund_id,
                "line_items": len(pcap_df),
                "performance_metrics": performance_metrics
            })
        else:
            print("No PCAP data generated")
            pcap_summary.set({
                "error": "No PCAP data found",
                "as_of_date": as_of_date,
                "fund": selected_fund_id
            })
            pcap_data.set(pd.DataFrame())
            
    except Exception as e:
        print(f"Error generating PCAP: {e}")
        import traceback
        traceback.print_exc()
        pcap_summary.set({"error": f"Error: {str(e)}", "as_of_date": input.pcap_as_of_date()})
        pcap_data.set(pd.DataFrame())