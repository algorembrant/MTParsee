import pandas as pd
import numpy as np
import scipy.stats as stats

def calculate_rolling_metrics(input_csv, output_csv):
    print(f"Reading data from: {input_csv}")
    
    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"Error: The file '{input_csv}' was not found.")
        return

    # ---------------------------------------------------------
    # 1. Preprocessing & Cleaning
    # ---------------------------------------------------------
    # Convert time columns to datetime objects
    df['Time_deal'] = pd.to_datetime(df['Time_deal'], errors='coerce')
    
    # Sort by time to ensure rolling metrics are chronological
    df = df.sort_values(by='Time_deal').reset_index(drop=True)

    # Ensure numeric columns are floats and handle missing values
    df['Profit'] = pd.to_numeric(df['Profit'], errors='coerce').fillna(0.0)
    df['Balance'] = pd.to_numeric(df['Balance'], errors='coerce').ffill()

    # Define helper series
    equity = df['Balance']
    profits = df['Profit']
    
    # Determine Initial Balance (Start of the window)
    # We assume the first record's balance is the starting point
    initial_balance = equity.iloc[0]

    # Calculate Time Elapsed in Years (for CAGR)
    start_time = df['Time_deal'].iloc[0]
    # Add small epsilon to prevent division by zero on the first row
    years_elapsed = (df['Time_deal'] - start_time).dt.total_seconds() / (365.25 * 24 * 3600)
    years_elapsed = years_elapsed.replace(0, 0.000001)

    # ---------------------------------------------------------
    # 2. Rolling (Expanding) Metric Calculations
    # ---------------------------------------------------------
    print("Calculating rolling metrics...")

    # --- Helpers for Win/Loss Stats ---
    # Create streams of just wins and just losses (positive)
    wins = profits.clip(lower=0)
    losses = profits.clip(upper=0).abs()
    
    expand_wins_sum = wins.expanding().sum()
    expand_losses_sum = losses.expanding().sum()
    expand_max_win = wins.expanding().max()
    expand_max_loss = losses.expanding().max()

    # --- Helper for Drawdown Stats ---
    hwm = equity.expanding().max()      # High Water Mark
    dd_dollar = hwm - equity            # Drawdown in $
    dd_pct = dd_dollar / hwm            # Drawdown in %
    
    dd_sq_mean = (dd_pct ** 2).expanding().mean() # For Ulcer Index
    dd_sq_sum = (dd_pct ** 2).expanding().sum()   # For Burke Ratio
    max_dd_pct = dd_pct.expanding().max()         # For Sterling Ratio

    # --- Helper for Returns Distribution (Risk of Ruin / Sharpe) ---
    pct_ret = equity.pct_change().fillna(0)
    roll_mean_ret = pct_ret.expanding().mean()
    roll_var_ret = pct_ret.expanding().var()
    roll_std_ret = pct_ret.expanding().std()
    roll_skew = pct_ret.expanding().skew().fillna(0)
    roll_kurt = pct_ret.expanding().kurt().fillna(0) # Excess kurtosis
    n_trades = profits.expanding().count()

    # ==========================================
    # Requested Metrics
    # ==========================================

    # 1. Expectancy (Average Profit)
    df['rolling_Expectancy_Expectancy'] = profits.expanding().mean()

    # 2. Gain-to-Pain Ratio (GPR) -> Sum(Wins) / Abs(Sum(Losses))
    df['rolling_GPR_GainToPainRatio'] = expand_wins_sum / expand_losses_sum.replace(0, np.nan)

    # 3. CAGR (Compound Annual Growth Rate)
    # Formula: (End_Equity / Start_Equity)^(1/years) - 1
    # We use abs() to handle negative equity scenarios safely
    df['rolling_CAGR_CompoundAnnualGrowthRate'] = (
        (equity / initial_balance).abs().pow(1 / years_elapsed) - 1
    )

    # 4. Martin Ratio -> CAGR / Ulcer Index
    ulcer_index = np.sqrt(dd_sq_mean)
    df['rolling_Martin_MartinRatio'] = df['rolling_CAGR_CompoundAnnualGrowthRate'] / ulcer_index.replace(0, np.nan)

    # 5. Sterling Ratio -> CAGR / Max Drawdown
    df['rolling_Sterling_SterlingRatio'] = df['rolling_CAGR_CompoundAnnualGrowthRate'] / max_dd_pct.replace(0, np.nan)

    # 6. Burke Ratio -> CAGR / Sqrt(Sum(Drawdown^2))
    burke_denom = np.sqrt(dd_sq_sum)
    df['rolling_Burke_BurkeRatio'] = df['rolling_CAGR_CompoundAnnualGrowthRate'] / burke_denom.replace(0, np.nan)

    # 7. Risk of Ruin -> exp(-2 * mean_ret / var_ret)
    # If mean return is negative, Ruin is 100% (1.0).
    ror = np.exp(-2 * roll_mean_ret / roll_var_ret)
    ror = np.where(roll_mean_ret < 0, 1.0, ror)
    df['rolling_RoR_RiskOfRuin'] = ror.clip(0, 1)

    # 8. Deflated Sharpe Ratio (DSR)
    # Using Probabilistic Sharpe Ratio (PSR) logic on the expanding window
    sr = roll_mean_ret / roll_std_ret
    # DSR Denominator term: 1 - skew*SR + ((kurt+2)/4)*SR^2
    dsr_denom_term = 1 - roll_skew * sr + ((roll_kurt + 2) / 4) * (sr**2)
    dsr_denom = np.sqrt(dsr_denom_term.abs())
    
    dsr_stat = (sr * np.sqrt(n_trades - 1)) / dsr_denom.replace(0, np.nan)
    df['rolling_DSR_DeflatedSharpeRatio'] = stats.norm.cdf(dsr_stat)

    # 9. Pain Index -> Mean Drawdown Depth
    df['rolling_PainIndex_PainIndex'] = dd_pct.expanding().mean()

    # 10. Pain Ratio -> CAGR / Pain Index
    df['rolling_PainRatio_PainRatio'] = df['rolling_CAGR_CompoundAnnualGrowthRate'] / df['rolling_PainIndex_PainIndex'].replace(0, np.nan)

    # 11. Lake Ratio -> Sum(Drawdown_Peaks) / Total Profit
    # Approximated here as Area Under Water / Total Profit
    df['rolling_Lake_LakeRatio'] = dd_dollar.expanding().sum() / profits.cumsum().replace(0, np.nan)

    # 12. Outlier Win/Loss Ratio (OWLR) -> Max Win / Max Loss
    df['rolling_OWLR_OutlierWinLossRatio'] = expand_max_win / expand_max_loss.replace(0, np.nan)

    # 13. Profitability Index -> Profit Factor (Gross Wins / Gross Losses)
    df['rolling_PI_ProfitabilityIndex'] = expand_wins_sum / expand_losses_sum.replace(0, np.nan)

    # ---------------------------------------------------------
    # 3. Export
    # ---------------------------------------------------------
    # Clean up Infinite values (divide by zero artifacts)
    metric_cols = [c for c in df.columns if 'rolling_' in c]
    df[metric_cols] = df[metric_cols].replace([np.inf, -np.inf], np.nan)

    df.to_csv(output_csv, index=False)
    print(f"Success! Processed data saved to: {output_csv}")

# --- Execution ---
if __name__ == "__main__":
    input_filename = 'merged_extracted_orders_and_deals.csv'
    output_filename = '5_layer_output.csv'
    
    calculate_rolling_metrics(input_filename, output_filename)