import pandas as pd
import numpy as np
import os
from scipy import stats

input_file = 'merged_extracted_orders_and_deals.csv'
output_file = '4_layer_output.csv'

if not os.path.exists(input_file):
    print(f"Error: File {input_file} not found.")
else:
    print("Loading data...")
    df = pd.read_csv(input_file)
    
    # Ensure numeric types
    df['Profit'] = pd.to_numeric(df['Profit'], errors='coerce').fillna(0)
    df['Balance'] = pd.to_numeric(df['Balance'], errors='coerce').fillna(0)

    # ==========================================
    # --- PREVIOUS COLUMNS CALCULATION ---
    # ==========================================
    df['Profitable Trade'] = np.where(df['Profit'] > 0, 1, 0)
    df['Unprofitable Trade'] = np.where(df['Profit'] < 0, 1, 0) 
    
    # Cumulative Counts (Internal variables)
    win_c = df['Profitable Trade'].cumsum()
    loss_c = df['Unprofitable Trade'].cumsum()

    # --- NEWLY REQUESTED COLUMNS ---
    df['rolling_profitable_trade'] = win_c
    df['rolling_unprofitable_trade'] = loss_c
    # -------------------------------
    
    # Basic Rolling Metrics
    df['rolling_winrate'] = (win_c / (win_c + loss_c)).fillna(0)
    df['rolling_gross_profit'] = df['Profit'].clip(lower=0).cumsum()
    df['rolling_gross_loss'] = df['Profit'].clip(upper=0).cumsum()
    df['rolling_net'] = df['Profit'].cumsum()
    
    # Rolling Profit Factor
    df['rolling_profit_factor'] = (df['rolling_gross_profit'] / df['rolling_gross_loss'].abs()).replace([np.inf, -np.inf], 0).fillna(0)
    
    # Drawdowns
    initial_bal = df['Balance'].iloc[0] - df['Profit'].iloc[0]
    df['rolling_balance_drawdown_absolute'] = (initial_bal - df['Balance'].cummin()).clip(lower=0)
    
    peaks = df['Balance'].cummax()
    dd_vals = peaks - df['Balance']
    df['rolling_balance_drawdown_maximal'] = dd_vals.cummax()
    df['rolling_balance_drawdown_relative'] = ((dd_vals / peaks.replace(0, np.nan)) * 100).fillna(0).cummax()
    
    df['rolling_total_deals'] = np.arange(1, len(df) + 1)
    df['rolling_win_count'] = win_c
    df['rolling_lose_count'] = loss_c
    df['rolling_total_trades'] = win_c + loss_c

    # Rolling Averages
    df['rolling_average_profit'] = (df['rolling_gross_profit'] / win_c.replace(0, np.nan)).fillna(0)
    df['rolling_average_loss'] = (df['rolling_gross_loss'] / loss_c.replace(0, np.nan)).fillna(0)
    df['rolling_expected_payoff'] = (df['rolling_net'] / df['rolling_total_trades'].replace(0, np.nan)).fillna(0)
    
    # Rolling LR Correlation
    print("Calculating Linear Regression (Correlation)...")
    corrs = []
    y = df['Balance'].values
    for i in range(1, len(df) + 1):
        if i < 2:
            corrs.append(0)
            continue
        xi = np.arange(i)
        yi = y[:i]
        slope, intercept, r_val, p_val, std_err = stats.linregress(xi, yi)
        corrs.append(r_val)
    df['rolling_LR_correlation'] = corrs

    # ==========================================
    # --- NEW 10 COLUMNS ADDITION ---
    # ==========================================
    print("Calculating new streak and trade type metrics...")

    # 1. Short vs Long Wins
    df['rolling_long_trades_won'] = np.where(
        (df['Type_order'].str.contains('buy', case=False, na=False)) & (df['Profitable Trade'] == 1), 1, 0
    ).cumsum()

    df['rolling_short_trades_won'] = np.where(
        (df['Type_order'].str.contains('sell', case=False, na=False)) & (df['Profitable Trade'] == 1), 1, 0
    ).cumsum()

    # 2. Largest Profit/Loss Trade
    df['rolling_largest_profit_trade'] = df['Profit'].clip(lower=0).cummax()
    df['rolling_largest_loss_trade'] = df['Profit'].clip(upper=0).cummin()

    # 3. Consecutive Metrics
    mask_trade = df['Profit'] != 0
    df_trades = df[mask_trade].copy()
    
    if not df_trades.empty:
        is_win = df_trades['Profit'] > 0
        is_loss = df_trades['Profit'] < 0
        
        group_id = (is_win != is_win.shift()).cumsum()
        df_trades['streak_counter'] = df_trades.groupby(group_id).cumcount() + 1
        
        df_trades['curr_win_streak'] = np.where(is_win, df_trades['streak_counter'], 0)
        df_trades['curr_loss_streak'] = np.where(is_loss, df_trades['streak_counter'], 0)
        
        df_trades['rolling_maximum_consecutive_wins'] = df_trades['curr_win_streak'].cummax()
        df_trades['rolling_maximum_consecutive_loses'] = df_trades['curr_loss_streak'].cummax()

        df_trades['streak_sum'] = df_trades.groupby(group_id)['Profit'].cumsum()
        df_trades['curr_win_streak_sum'] = np.where(is_win, df_trades['streak_sum'], 0)
        df_trades['curr_loss_streak_sum'] = np.where(is_loss, df_trades['streak_sum'], 0)
        
        df_trades['rolling_maximal_consecutive_profit'] = df_trades['curr_win_streak_sum'].cummax()
        df_trades['rolling_maximal_consecutive_loss'] = df_trades['curr_loss_streak_sum'].cummin()

        # Identify start of a streak
        win_start = (is_win) & (~is_win.shift(1).fillna(False))
        loss_start = (is_loss) & (~is_loss.shift(1).fillna(False))
        
        total_wins = is_win.cumsum()
        total_win_streaks = win_start.cumsum()
        total_losses = is_loss.cumsum()
        total_loss_streaks = loss_start.cumsum()
        
        df_trades['rolling_average_consecutive_wins'] = (total_wins / total_win_streaks.replace(0, np.nan)).fillna(0)
        df_trades['rolling_average_consecutive_loses'] = (total_losses / total_loss_streaks.replace(0, np.nan)).fillna(0)

        new_cols = [
            'rolling_maximum_consecutive_wins', 'rolling_maximum_consecutive_loses',
            'rolling_maximal_consecutive_profit', 'rolling_maximal_consecutive_loss',
            'rolling_average_consecutive_wins', 'rolling_average_consecutive_loses'
        ]
        
        df = df.join(df_trades[new_cols])
        df[new_cols] = df[new_cols].ffill().fillna(0)
    else:
        for col in ['rolling_maximum_consecutive_wins', 'rolling_maximum_consecutive_loses', 
                    'rolling_maximal_consecutive_profit', 'rolling_maximal_consecutive_loss', 
                    'rollling_average_consecutive_wins', 'rolling_average_consecutive_loses']:
            df[col] = 0

    # ==========================================
    # --- SAVE OUTPUT ---
    # ==========================================
    df.to_csv(output_file, index=False)
    print(f"Success! Processed {len(df)} rows.")
    print(f"Saved to: {output_file}")