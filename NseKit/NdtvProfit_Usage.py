#=====================================================================#
#                               NdtvProfit Usage
#=====================================================================#


from NseKit import NdtvProfit
from rich.console import Console

convert_to_cr       = True
filter_old_expiry   = True

# Create NDTV Profit instance with global settings
api = NdtvProfit(convert_to_cr=convert_to_cr, filter_old_expiry=filter_old_expiry)
# api = NdtvProfit(convert_to_cr=False, filter_old_expiry=True)
# api = NdtvProfit(False, True)
rich = Console()

#---------------------------------------------------------- Summary ----------------------------------------------------------

# 🔹 Nifty Future Summary
# print(api.get_nifty_summary())                                                            # Default: NIFTY 50
# print(api.get_nifty_summary("NIFTY BANK"))                                                # Specific Index

# 🔹 Stock Reference Details
# print(api.get_stock_details("Nse"))                                                       # "All" | "Nse"

#---------------------------------------------------------- Options Data ----------------------------------------------------------

# 🔹 Most Active Options by Volume
# print(api.get_most_active_options_by_volume("call", "stock", limit=20))                   # "call"|"put", "stock"|"index"
# print(api.get_most_active_options_by_volume("put", "index", limit=20, output="json"))

# 🔹 Top Options by Open Interest (OI)
# print(api.get_top_open_interest("call", "stock", limit=10))                               # "call"|"put", "stock"|"index"
# print(api.get_top_open_interest("put", "index", limit=5))

# 🔹 Current F&O Open Interest Break-Up
# print(api.get_oi_breakup("stock"))                                                        # "stock"|"index"

# 🔹 F&O Open Interest Change Since Last Expiry
# print(api.get_oi_change_since_last_expiry("stock"))                                       # "stock"|"index"

# 🔹 PCR (Put Call Ratio) Data
# print(api.get_pcr_data("stock"))                                                          # "stock" | "index"

#---------------------------------------------------------- Futures Data ----------------------------------------------------------

# 🔹 Future OI Gainers/Losers
# print(api.get_future_by_oi("up", limit=50))                               # "up" | "down"

# 🔹 Future Premium/Discount
# print(api.get_future_by_premium_discount("premium", limit=50))            # "premium"|"discount"

# 🔹 Future Rollover
# print(api.get_future_by_rollover("highest", limit=50))                    # "highest" | "lowest"

# 🔹 Buildups (Hedging)
# print(api.get_buildups("long_buildup", limit=50))                         # "long_buildup" | "long_unwinding" | "short_covering" | "short_buildup"

# 🔹 Future Active Volume
# print(api.get_future_active_volume(show_large_change=False, limit=50))    # Most Active
# print(api.get_future_active_volume(show_large_change=True, limit=50))     # Large Volume Change

#---------------------------------------------------------- Sectoral Movement ----------------------------------------------------------

# 🔹 Sectoral Movement
# print(api.get_sectoral_movement())                                        # Comprehensive sectoral price/OI/Vol trends

#---------------------------------------------------------- Customization ----------------------------------------------------------

# 🔹 JSON Output (Raw Data)
# json_data = api.get_pcr_data("index", output="json")
# rich.print(json_data[0])                                                  # Access raw API keys

# 🔹 Custom Column Ordering
# custom_order = ["Symbol", "OI", "OI Chg %", "Spot Price"]
# df = api.get_options_by_oi("call", "stock", column_order=custom_order)
# print(df.head())

# 🔹 Custom Header Mapping
# custom_map = {"symbol": "Ticker", "open-interest": "Total OI"}
# df = api.get_pcr_data("stock", header_map=custom_map)
# print(df.head())
