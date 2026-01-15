import pandas as pd
import numpy as np
from scipy import stats
import sys

def calculate_rolling_metrics(input_file, output_file):
    try:
        print(f"Reading {input_file}...")
        df = pd.read_csv(input_file)
        
        # 1. Data Preprocessing
        # Ensure Time_deal is datetime
        df['Time_deal'] = pd.to_datetime(df['Time_deal'])
        
        # Sort by time to ensure rolling calculations are correct
        df = df.sort_values('Time_deal').reset_index(drop=True)
        
        # Calculate 'Trade Return' for each row
        # Balance in the CSV is usually the balance *after* the deal.
        # Previous Balance = Current Balance - Profit
        df['Prev_Balance'] = df['Balance'] - df['Profit']
        
        # Avoid division by zero
        df['Trade_Return'] = np.where(
            df['Prev_Balance'] > 0, 
            df['Profit'] / df['Prev_Balance'], 
            0.0
        )
        
        # 2. Define Metric Columns (using requested format)
        metric_map = {
            'Sharpe': 'rolling_Sharpe_Ratio',
            'Sortino': 'rolling_Sortino_Ratio',
            'Calmar': 'rolling_Calmar_Ratio',
            'Stability': 'rolling_Stability',
            'Recovery Factor': 'rolling_Recovery_Factor',
            'omega ratio': 'rolling_Omega_Ratio',
            'skew': 'rolling_Skewness',
            'kurtosis': 'rolling_Kurtosis',
            'tail ratio': 'rolling_Tail_Ratio',
            'alpha': 'rolling_Alpha',
            'beta': 'rolling_Beta',
            'Common Sense Ratio': 'rolling_Common_Sense_Ratio',
            'volatility': 'rolling_Volatility',
            'Kelly Criterion': 'rolling_Kelly_Criterion',
            'System Quality Number (SQN)': 'rolling_SQN_System_Quality_Number',
            'K-Ratio': 'rolling_K_Ratio',
            'R-Squared': 'rolling_R_Squared',
            'CPC Index': 'rolling_CPC_Index',
            'VaR': 'rolling_VaR_Value_at_Risk',
            'CVaR': 'rolling_CVaR_Conditional_Value_at_Risk',
            'return standard deviation': 'rolling_Return_Standard_Deviation',
            'AHPR': 'rolling_AHPR_Average_Holding_Period_Return',
            'GHPR': 'rolling_GHPR_Geometric_Holding_Period_Return',
            'Drawdown duration': 'rolling_Drawdown_Duration',
            'Maximum drawdown duration': 'rolling_Max_Drawdown_Duration',
            'Time-weighted return (TWR)': 'rolling_TWR_Time_Weighted_Return',
            'Money-weighted return (MWR / IRR)': 'rolling_MWR_Money_Weighted_Return',
            'Ulcer Index': 'rolling_Ulcer_Index',
            'MAE': 'rolling_MAE_Max_Adverse_Excursion',
            'MFE': 'rolling_MFE_Max_Favorable_Excursion',
            'MAR Ratio': 'rolling_MAR_Ratio',
            'Information Ratio': 'rolling_Information_Ratio',
            'Treynor Ratio': 'rolling_Treynor_Ratio',
            'Tracking Error': 'rolling_Tracking_Error',
            'Active Share': 'rolling_Active_Share'
        }

        # Initialize new columns with NaN
        for col in metric_map.values():
            df[col] = np.nan

        # 3. Expanding Window Calculation
        print("Calculating metrics (this may take a moment)...")
        
        # Pre-convert columns to numpy arrays for speed in loop
        returns = df['Trade_Return'].values
        profits = df['Profit'].values
        balances = df['Balance'].values
        times = df['Time_deal'].values
        
        n = len(df)
        
        # State variables for expensive tracking
        max_dd_duration_sec = 0.0
        
        for i in range(n):
            # Slices for expanding window (0 to i)
            hist_rets = returns[:i+1]
            hist_profits = profits[:i+1]
            hist_bal = balances[:i+1]
            current_time = times[i]
            
            # --- Basic Return Stats ---
            if len(hist_rets) > 1:
                mean_ret = np.mean(hist_rets)
                std_ret = np.std(hist_rets, ddof=1)
            else:
                mean_ret = hist_rets[0]
                std_ret = 0.0

            # Volatility
            df.at[i, metric_map['volatility']] = std_ret
            df.at[i, metric_map['return standard deviation']] = std_ret
            
            # Sharpe (Assuming Risk Free = 0, Simple Trade Sharpe)
            if std_ret > 1e-9:
                df.at[i, metric_map['Sharpe']] = mean_ret / std_ret
            else:
                df.at[i, metric_map['Sharpe']] = 0.0

            # Sortino
            neg_rets = hist_rets[hist_rets < 0]
            if len(neg_rets) > 1:
                down_std = np.std(neg_rets, ddof=1)
                if down_std > 1e-9:
                    df.at[i, metric_map['Sortino']] = mean_ret / down_std
            
            # Skew / Kurtosis
            if len(hist_rets) > 2:
                df.at[i, metric_map['skew']] = stats.skew(hist_rets)
                df.at[i, metric_map['kurtosis']] = stats.kurtosis(hist_rets)

            # --- Equity Curve Stats (Drawdown) ---
            cum_max = np.maximum.accumulate(hist_bal)
            drawdowns = (cum_max - hist_bal) / cum_max
            max_dd = np.max(drawdowns)
            
            # Ulcer Index
            if len(drawdowns) > 0:
                df.at[i, metric_map['Ulcer Index']] = np.sqrt(np.mean(drawdowns**2))

            # Calmar / MAR Ratio (Approximated with simple CAGR)
            # Calculate years elapsed
            elapsed_seconds = (current_time - times[0]).astype('timedelta64[s]').astype(float)
            years = elapsed_seconds / (365 * 24 * 3600)
            
            total_return = (hist_bal[-1] / (hist_bal[0] - hist_profits[0])) - 1 if (hist_bal[0] - hist_profits[0]) > 0 else 0
            
            cagr = 0
            if years > 0:
                # Handle negative base for power
                if total_return > -1:
                    cagr = (1 + total_return) ** (1 / years) - 1
            
            if max_dd > 0:
                df.at[i, metric_map['Calmar']] = cagr / max_dd
                df.at[i, metric_map['MAR Ratio']] = cagr / max_dd

            # Recovery Factor (Net Profit / Max Drawdown Amount)
            net_profit = np.sum(hist_profits)
            dd_amounts = cum_max - hist_bal
            max_dd_amt = np.max(dd_amounts)
            
            if max_dd_amt > 0:
                df.at[i, metric_map['Recovery Factor']] = net_profit / max_dd_amt

            # Drawdown Duration
            # Current DD duration: Time since last High Water Mark
            hwm_idx = np.argmax(hist_bal)
            current_dd_duration = (current_time - times[hwm_idx]).astype('timedelta64[s]').astype(float)
            df.at[i, metric_map['Drawdown duration']] = current_dd_duration
            
            # Update Max Drawdown Duration
            max_dd_duration_sec = max(max_dd_duration_sec, current_dd_duration)
            df.at[i, metric_map['Maximum drawdown duration']] = max_dd_duration_sec

            # Stability (R-Squared of Equity Log Linearity)
            if len(hist_bal) > 2:
                try:
                    # Log of equity (handle negatives/zeros)
                    y = np.log(np.abs(hist_bal) + 1e-9) 
                    x = np.arange(len(y))
                    slope, intercept, r_val, p_val, std_err = stats.linregress(x, y)
                    df.at[i, metric_map['Stability']] = r_val ** 2
                    
                    # K-Ratio (Slope / StdErr of equity curve)
                    slope_k, _, _, _, std_err_k = stats.linregress(x, hist_bal)
                    if std_err_k > 0:
                        df.at[i, metric_map['K-Ratio']] = slope_k / std_err_k
                except:
                    pass

            # --- Trade Stats ---
            wins = hist_profits[hist_profits > 0]
            losses = np.abs(hist_profits[hist_profits < 0])
            
            # Omega Ratio
            if np.sum(losses) > 0:
                df.at[i, metric_map['omega ratio']] = np.sum(wins) / np.sum(losses)

            # Kelly & CPC
            if len(wins) > 0 and len(losses) > 0:
                win_rate = len(wins) / len(hist_profits)
                avg_win = np.mean(wins)
                avg_loss = np.mean(losses)
                if avg_loss > 0:
                    b_ratio = avg_win / avg_loss
                    # Kelly = p - q/b
                    df.at[i, metric_map['Kelly Criterion']] = win_rate - (1 - win_rate) / b_ratio
                    
                    # CPC Index = ProfitFactor * WinRate * PayoffRatio
                    pf = np.sum(wins) / np.sum(losses)
                    df.at[i, metric_map['CPC Index']] = pf * win_rate * b_ratio

            # SQN
            if len(hist_profits) > 1:
                std_profit = np.std(hist_profits, ddof=1)
                if std_profit > 0:
                    sqn = np.sqrt(len(hist_profits)) * np.mean(hist_profits) / std_profit
                    df.at[i, metric_map['System Quality Number (SQN)']] = sqn

            # Tail Ratio & VaR
            if len(hist_rets) > 10:
                t_95 = np.percentile(hist_rets, 95)
                t_05 = np.abs(np.percentile(hist_rets, 5))
                if t_05 > 0:
                    df.at[i, metric_map['tail ratio']] = t_95 / t_05
                    
                    # Common Sense Ratio = Profit Factor * Tail Ratio
                    pf = np.sum(wins)/np.sum(losses) if np.sum(losses) > 0 else 0
                    df.at[i, metric_map['Common Sense Ratio']] = pf * (t_95 / t_05)
                
                # VaR (5%)
                var_val = np.percentile(hist_rets, 5)
                df.at[i, metric_map['VaR']] = var_val
                
                # CVaR (Mean of returns <= VaR)
                cvar_vals = hist_rets[hist_rets <= var_val]
                if len(cvar_vals) > 0:
                    df.at[i, metric_map['CVaR']] = np.mean(cvar_vals)

            # AHPR / GHPR / TWR
            df.at[i, metric_map['AHPR']] = np.mean(1 + hist_rets)
            if np.all((1 + hist_rets) > 0):
                df.at[i, metric_map['GHPR']] = stats.gmean(1 + hist_rets)
            
            df.at[i, metric_map['Time-weighted return (TWR)']] = np.prod(1 + hist_rets) - 1

        # 4. Cleanup & Save
        # Drop temporary calculation columns
        df = df.drop(columns=['Prev_Balance', 'Trade_Return'])
        
        print(f"Saving to {output_file}...")
        df.to_csv(output_file, index=False)
        print("Done.")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    calculate_rolling_metrics(
        'merged_extracted_orders_and_deals.csv', 
        '7_layer_output.csv'
    )