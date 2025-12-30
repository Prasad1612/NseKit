#=====================================================================#
#                                   NseKit
#=====================================================================#

from NseKit import NseKit, Moneycontrol
from rich.console import Console

# Create NSE instance
mc = Moneycontrol.MC()
get = NseKit.Nse()
rich = Console()

# #---------------------------------------------------------- NSE Data ----------------------------------------------------------

# # 🔹 Market Status
# print(get.nse_market_status("Nifty50"))                                                   # "Market Status" | "Mcap" | "Nifty50" | "Gift Nifty"
     
# rich.print(get.nse_is_market_open("Capital Market"), "\n")                                # "Capital Market" | "Currency" | "Commodity" | "Debt" | "currencyfuture"


# # 🔹 Trading Holidays
# print(get.nse_trading_holidays())                                                         # Trading holidays DataFrame
# print(get.nse_trading_holidays(list_only=True))                                           # List of trading holiday dates

# # 🔹 Clearing Holidays
# print(get.nse_clearing_holidays())                                                        # Clearing holidays DataFrame
# print(get.nse_clearing_holidays(list_only=True))                                          # List of clearing holiday dates

# # 🔹 Check Trading Holiday
# print(get.is_nse_trading_holiday())                                                       # Check if today is a trading holiday
# print(get.is_nse_trading_holiday("21-Oct-2025"))                                          # Check if specific date is a trading holiday

# # 🔹 Check Clearing Holiday
# print(get.is_nse_clearing_holiday())                                                      # Check if today is a clearing holiday
# print(get.is_nse_clearing_holiday("22-Oct-2025"))                                         # Check if specific date is a clearing holiday

# # 🔹 Live Market Turnover
# print(get.nse_live_market_turnover())                                                     # Live market turnover summary

# # 🔹 Historical Circulars
# print(get.nse_live_hist_circulars())                                                      # Default: yesterday to today
# print(get.nse_live_hist_circulars("18-07-2025", "18-10-2025"))                            # Specific date range
# print(get.nse_live_hist_circulars("NSE Listing"))                                         # Filter by department


# # 🔹 Historical Press Releases
# print(get.nse_live_hist_press_releases())                                                 # Default: yesterday to today
# print(get.nse_live_hist_press_releases("18-07-2025", "18-10-2025"))                       # Specific date range
# print(get.nse_live_hist_press_releases("01-10-2025", "4-10-2025", "NSE Listing"))         # Filter by department
# '''
# Corporate Communications , Investor Services Cell , Member Compliance , NSE Clearing , NSE Indices , NSE Listing , Surveillance 
# '''

# # 🔹 Reference Rates
# print(get.nse_reference_rates())                                                          # Currency reference rates

# # 🔹 Top 10 Nifty 50
# print(get.nse_eod_top10_nifty50("17-10-25"))                                              # Top 10 Nifty 50 for specific trade date (DD-MM-YY)

# # 🔹 Nifty 50 List
# print(get.nse_6m_nifty_50())                                                              # Nifty 50 constituents
# print(get.nse_6m_nifty_50(list_only=True))                                                # List of Nifty 50 symbols

# # 🔹 F&O Full List
# print(get.nse_eom_fno_full_list())                                                        # Full Stock F&O DataFrame
# print(get.nse_eom_fno_full_list(list_only=True))                                          # Stock F&O symbols list only
# print(get.nse_eom_fno_full_list("index"))                                                 # Full Index F&O DataFrame
# print(get.nse_eom_fno_full_list("index", list_only=True))                                 # Index F&O symbols list only

# # 🔹 Nifty 500 List
# print(get.nse_6m_nifty_500())                                                             # Nifty 500 constituents
# print(get.nse_6m_nifty_500(list_only=True))                                               # List of Nifty 500 symbols

# # 🔹 Equity Full List
# print(get.nse_eod_equity_full_list())                                                     # Full equity list
# print(get.nse_eod_equity_full_list(list_only=True))                                       # List of equity symbols

# print(get.state_wise_registered_investors())                                              # state wise registered investors


# #---------------------------------------------------------- IPO Data ----------------------------------------------------------#

# # 🔹 Currently Open IPOs
# print(get.ipo_current())

# # 🔹 Today's Special Pre-Open Session (Newly Listed IPOs)
# print(get.ipo_preopen())

# # 🔹 IPO Tracker Summary
# print(get.ipo_tracker_summary())                                                          # All YTD IPOs
# print(get.ipo_tracker_summary("SME"))                                                     # SME IPOs Only
# print(get.ipo_tracker_summary("Mainboard"))                                               # Mainboard IPOs Only



# #---------------------------------------------------------- Pre-Open Market ----------------------------------------------------------

# # 🔹 Pre-Open Index Info
# print(get.pre_market_nifty_info("NIFTY 50"))                                              # Index A/D Summary, "Nifty Bank" | "Emerge" | "Securities in F&O" | "Others" | "All"

# # 🔹 All NSE Pre-Open Advances/Declines Summary
# print(get.pre_market_all_nse_adv_dec_info())                                              # NSE-wide Advance/Decline Data

# # 🔹 Pre-Open Market Stocks (All Categories)
# print(get.pre_market_info("All"))                                                         # All Pre-Open Stocks
# print(get.pre_market_info("NIFTY 50"))                                                    # Nifty 50 Pre-Open
# print(get.pre_market_info("Nifty Bank"))                                                  # Bank Nifty Pre-Open
# print(get.pre_market_info("Emerge"))                                                      # SME Pre-Open
# print(get.pre_market_info("Securities in F&O"))                                           # F&O Stocks Pre-Open

# # 🔹 Pre-Open Market Derivatives
# print(get.pre_market_derivatives_info("Stock Futures"))                                   # Derivatives Pre-Open data "Index Futures" | "Stock Futures"

# #---------------------------------------------------------- Index Live Data ----------------------------------------------------------

# # 🔹 All NSE Indices Live Data
# print(get.index_live_all_indices_data())                                                  # All Indices Live Snapshot

# # 🔹 Specific Index Constituents
# print(get.index_live_indices_stocks_data("NIFTY 50"))                                     # Nifty 50 Stocks DataFrame
# print(get.index_live_indices_stocks_data("NIFTY IT", list_only=True))                     # Only Nifty 50 Symbols

# # 🔹 Nifty 50 Returns Summary
# print(get.index_live_nifty_50_returns())                                                  # 1W–5Y Nifty Return %

# # 🔹 Index Contribution Data
# print(get.index_live_contribution())                                                      # Nifty 50 Stock-wise Index Contribution
# print(get.index_live_contribution("Full"))                                                # Nifty 50 Full Stock-wise Index Contribution
# print(get.index_live_contribution("NIFTY IT"))                                            # Index Stock-wise Contribution
# print(get.index_live_contribution("NIFTY IT","Full"))                                     # Index Full Stock-wise Index Contribution



# #---------------------------------------------------------- Index_Eod_Data ----------------------------------------------------------------#

# # 🔹 Fetch NSE Index EOD Bhavcopy for a specific date
# print(get.index_eod_bhav_copy("17-10-2025"))                                              # Returns DataFrame of all indices for that date

# # 🔹 Fetch Historical Index Data (OHLC + Turnover)
# print(get.index_historical_data("NIFTY 50", "01-01-2025", "17-10-2025"))
# print(get.index_historical_data("NIFTY 50", "01-12-2025"))                                # Auto today date as "To date" 
# print(get.index_historical_data("NIFTY BANK", "1W"))                                      # Last 1 Week      '1D','1W','1M','3M','6M','1Y','2Y','5Y','10Y','YTD','MAX'

# # 🔹 Fetch Historical Index P/E, P/B, Dividend Yield
# print(get.index_pe_pb_div_historical_data("NIFTY 50", "01-01-2025", "17-10-2025"))
# print(get.index_pe_pb_div_historical_data("NIFTY 50", "01-12-2025"))                      # Auto today date as "To date" 
# print(get.index_pe_pb_div_historical_data("NIFTY BANK", "1Y"))                            # Last 1 year      '1D','1W','1M','3M','6M','1Y','2Y','5Y','10Y','YTD','MAX'

# # 🔹 Fetch Historical India VIX Data
# print(get.india_vix_historical_data("01-08-2025", "17-10-2025"))                          # Direct date range
# print(get.india_vix_historical_data("1M"))                                                # Last 6 months     "1M", "3M", "6M", "1Y", "2Y", "5Y", "10Y", "YTD", "MAX"


# #---------------------------------------------------------- Live Gift Nifty & USDINR ----------------------------------------------------------------

# # 🔹 Fetch live Gift Nifty & USDINR data
# print(get.cm_live_gifty_nifty())                                                          #  Gift Nifty & USDINR data

# #---------------------------------------------------------- Live Market Statistics ----------------------------------------------------------------

# # 🔹 Fetch live Capital Market statistics from NSE
# print(get.cm_live_market_statistics())                                                    #  Capital Market statistics



# #---------------------------------------------------------- Capital Market Live Data ----------------------------------------------------------

# # 🔹 Equity Information
# print(get.cm_live_equity_info("RELIANCE"))                                                # Equity details for a symbol

# # 🔹 Equity Price Information
# print(get.cm_live_equity_price_info("RELIANCE"))                                          # Detailed price data with bid/ask levels

# # 🔹 Most Active Equities by Value
# print(get.cm_live_most_active_equity_by_value())                                          # Most active equities by traded value

# # 🔹 Most Active Equities by Volume
# print(get.cm_live_most_active_equity_by_vol())                                            # Most active equities by traded volume

# # 🔹 Volume Spurts
# print(get.cm_live_volume_spurts())                                                        # Volume Spurts

# # 🔹 52-Week High
# print(get.cm_live_52week_high())                                                          # Stocks hitting 52-week highs

# # 🔹 52-Week Low
# print(get.cm_live_52week_low())                                                           # Stocks hitting 52-week lows

# # 🔹 Block Deals
# print(get.cm_live_block_deal())                                                           # Recent block deal data

# # 🔹 Insider Trading
# print(get.cm_live_hist_insider_trading())                                                 # Today's Insider trading  
# print(get.cm_live_hist_insider_trading("1M"))                                             # period                 "1D", "1W", "1M", "3M", "6M", "1Y"
# print(get.cm_live_hist_insider_trading("01-01-2025", "15-10-2025"))                       # Date range
# print(get.cm_live_hist_insider_trading("RELIANCE"))                                       # Today Insider trading for a symbol
# print(get.cm_live_hist_insider_trading("RELIANCE", "1M"))                                 # Symbol with period     "1D", "1W", "1M", "3M", "6M", "1Y"
# print(get.cm_live_hist_insider_trading("RELIANCE", "01-01-2025", "15-10-2025"))           # Symbol + date range

# # 🔹 Corporate Announcements
# print(get.cm_live_hist_corporate_announcement())                                          # Corporate announcements 
# print(get.cm_live_hist_corporate_announcement("12-10-2025", "15-10-2025"))                # Date range 
# print(get.cm_live_hist_corporate_announcement("RELIANCE"))                                # Announcements for a symbol
# print(get.cm_live_hist_corporate_announcement("RELIANCE", "01-01-2025", "15-10-2025"))    # Symbol + date range

# # 🔹 Corporate Actions
# print(get.cm_live_hist_corporate_action())                                                # Corporate actions (default: next 90 days)  
# print(get.cm_live_hist_corporate_action("1M"))                                            # period                 "1D", "1W", "1M", "3M", "6M", "1Y"
# print(get.cm_live_hist_corporate_action("01-01-2025", "15-10-2025"))                      # Date range
# print(get.cm_live_hist_corporate_action("LAURUSLABS"))                                    # Symbol Corporate actions
# print(get.cm_live_hist_corporate_action("LAURUSLABS", "1Y"))                              # Symbol with period     "1D", "1W", "1M", "3M", "6M", "1Y"
# print(get.cm_live_hist_corporate_action("RELIANCE", "01-01-2025", "15-10-2025"))          # Symbol + date range
# print(get.cm_live_hist_corporate_action("01-01-2025", "15-03-2025", "Dividend"))          # Filter by date and purpose


# # 🔹 Today's Event Calendar
# print(get.cm_live_today_event_calendar())                                                 # Today's corporate events
# print(get.cm_live_today_event_calendar("01-01-2025", "01-01-2025"))                       # Specific date range

# # 🔹 Upcoming Event Calendar
# print(get.cm_live_upcoming_event_calendar())                                              # Upcoming corporate events

# # 🔹 Board Meetings
# print(get.cm_live_hist_board_meetings())                                                  # Board meetings
# print(get.cm_live_hist_board_meetings("01-01-2025", "15-10-2025"))                        # Date range
# print(get.cm_live_hist_board_meetings("RELIANCE"))                                        # Board meetings for a symbol
# print(get.cm_live_hist_board_meetings("RELIANCE", "01-01-2025", "15-10-2025"))            # Symbol + date range

# # 🔹 Shareholder Meetings
# print(get.cm_live_hist_Shareholder_meetings())                                            # Shareholder meetings
# print(get.cm_live_hist_Shareholder_meetings("01-01-2025", "15-10-2025"))                  # Date range
# print(get.cm_live_hist_Shareholder_meetings("RELIANCE"))                                  # Shareholder meetings for a symbol
# print(get.cm_live_hist_Shareholder_meetings("RELIANCE", "01-01-2025", "15-10-2025"))      # Symbol + date range

# # 🔹 Qualified Institutional Placement (QIP)
# print(get.cm_live_hist_qualified_institutional_placement("In-Principle"))
# print(get.cm_live_hist_qualified_institutional_placement("Listing Stage"))
# print(get.cm_live_hist_qualified_institutional_placement("In-Principle", "1Y"))           # QIP for a period: "1D", "1W", "1M", "3M", "6M", "1Y"
# print(get.cm_live_hist_qualified_institutional_placement("Listing Stage", "1Y"))
# print(get.cm_live_hist_qualified_institutional_placement("In-Principle", "01-01-2025", "15-10-2025"))
# print(get.cm_live_hist_qualified_institutional_placement("Listing Stage", "01-01-2025", "15-10-2025"))

# print(get.cm_live_hist_qualified_institutional_placement("RELIANCE"))                                                 # QIP for a symbol
# print(get.cm_live_hist_qualified_institutional_placement("In-Principle", "RELIANCE", "01-01-2025"))                   # Auto today date as "To date"
# print(get.cm_live_hist_qualified_institutional_placement("In-Principle", "RELIANCE", "01-01-2025", "15-10-2025"))     # Symbol + date range + stage

# # 🔹 Preferential Issue
# print(get.cm_live_hist_preferential_issue("In-Principle"))
# print(get.cm_live_hist_preferential_issue("Listing Stage"))
# print(get.cm_live_hist_preferential_issue("In-Principle", "1Y"))                          # Preferential issue for a period: "1D", "1W", "1M", "3M", "6M", "1Y"
# print(get.cm_live_hist_preferential_issue("Listing Stage", "1Y"))
# print(get.cm_live_hist_preferential_issue("In-Principle", "01-01-2025", "15-10-2025"))
# print(get.cm_live_hist_preferential_issue("Listing Stage", "01-01-2025", "15-10-2025"))

# print(get.cm_live_hist_preferential_issue("RELIANCE"))                                                                # Preferential issue for a symbol
# print(get.cm_live_hist_preferential_issue("In-Principle", "RELIANCE", "01-01-2025"))                                  # Auto today date as "To date"
# print(get.cm_live_hist_preferential_issue("In-Principle", "RELIANCE", "01-01-2025", "15-10-2025"))                    # Symbol + date range + stage

# # 🔹 Right Issue
# print(get.cm_live_hist_right_issue("In-Principle"))
# print(get.cm_live_hist_right_issue("Listing Stage"))
# print(get.cm_live_hist_right_issue("In-Principle", "1Y"))                                 # Right issue for a period: "1D", "1W", "1M", "3M", "6M", "1Y"
# print(get.cm_live_hist_right_issue("Listing Stage", "1Y"))
# print(get.cm_live_hist_right_issue("In-Principle", "01-01-2025", "15-10-2025"))
# print(get.cm_live_hist_right_issue("Listing Stage", "01-01-2025", "15-10-2025"))

# print(get.cm_live_hist_right_issue("RELIANCE"))                                                                       # Right issue for a symbol
# print(get.cm_live_hist_right_issue("In-Principle", "RELIANCE", "01-01-2025"))                                         # Auto today date as "To date"
# print(get.cm_live_hist_right_issue("In-Principle", "RELIANCE", "01-01-2025", "15-10-2025"))                           # Symbol + date range + stage


# # 🔹 Voting Results
# print(get.cm_live_voting_results())                                                       # Corporate voting results

# # 🔹 Quarterly Shareholding Patterns
# print(get.cm_live_qtly_shareholding_patterns())                                           # Quarterly shareholding patterns

# # 🔹 Annual Reports
# print(get.cm_live_hist_annual_reports())                                                  # All Annual reports
# print(get.cm_live_hist_annual_reports("RELIANCE"))                                        # Annual reports for a symbol
# print(get.cm_live_hist_annual_reports("01-01-2025", "15-10-2025"))                        # Date range
# print(get.cm_live_hist_annual_reports("RELIANCE", "01-01-2025", "15-10-2025"))            # Symbol + date range


# #---------------------------------------------------------- Live Chart Data ----------------------------------------------------------

# print(get.nifty_chart("1D"))
# print(get.stock_chart("RELIANCE", "1D"))
# print(get.fno_chart("TCS", "FUTSTK","30-12-2025"))
# print(get.fno_chart("NIFTY", "OPTIDX","09-12-2025","CE25900"))

# #---------------------------------------------------------- FnO Live Data ----------------------------------------------------------

# # 🔹 All Futures & Options contracts Data                 
# #           (JSON format only)
# print(get.symbol_full_fno_live_data("TCS"))                                               
# print(get.symbol_specific_most_active_Calls_or_Puts_or_Contracts_by_OI("TCS","C"))        # Most Active Calls "C" | Puts "P" | Contracts by Open Interest (OI) "O"
# print(get.identifier_based_fno_contracts_live_chart_data("OPTSTKTCS30-12-2025CE3300.00")) 

# # 🔹 Futures Data
# print(get.fno_live_futures_data("RELIANCE"))                                              # Stock futures data for a symbol
# print(get.fno_live_futures_data("NIFTY"))                                                 # Index futures data for a symbol

# # 🔹 Top 20 Contracts Stock Futures, Stock Options
# print(get.fno_live_top_20_derivatives_contracts("Stock Futures"))                         # Top 20 Contracts       "Stock Futures" | "Stock Options"


# # 🔹 Most Active Futures Contracts by Volume
# print(get.fno_live_most_active_futures_contracts("Volume"))                               # Most active futures by volume
# print(get.fno_live_most_active_futures_contracts("Value"))                                # Most active futures by Value


# # 🔹 Most Active
# print(get.fno_live_most_active("Index", "Call", "Volume"))                                # Most active index call options
# print(get.fno_live_most_active("Index", "Call", "Value"))                                 # Most active index calls by traded value 
# print(get.fno_live_most_active("Index", "Put", "Volume"))                                 # Most active index put options
# print(get.fno_live_most_active("Index", "Put", "Value"))                                  # Most active index puts by traded value

# print(get.fno_live_most_active("Stock", "Call", "Volume"))                                # Most active stock call options
# print(get.fno_live_most_active("Stock", "Call", "Value"))                                 # Most active stock calls by traded value
# print(get.fno_live_most_active("Stock", "Put", "Volume"))                                 # Most active stock put options
# print(get.fno_live_most_active("Stock", "Put", "Value"))                                  # Most active stock puts by traded value 


# # 🔹 Most Active Contracts by Open Interest
# print(get.fno_live_most_active_contracts_by_oi())                                         # Most active contracts by open interest

# # 🔹 Most Active Contracts by Volume
# print(get.fno_live_most_active_contracts_by_volume())                                     # Most active contracts by volume

# # 🔹 Most Active Options Contracts by Volume
# print(get.fno_live_most_active_options_contracts_by_volume())                             # Most active options by volume

# # 🔹 Most Active Underlying
# print(get.fno_live_most_active_underlying())                                              # Most Active Underlying

# # 🔹 Change in Open Interest
# print(get.fno_live_change_in_oi())                                                        # Change in Open Interest

# # 🔹 Price Vs OI
# print(get.fno_live_oi_vs_price())                                                         # Price Vs OI   (Rise in OI and Rise in Price, Rise in OI and Slide in Price, etc)

# 🔹 Expiry Date
# print(get.fno_expiry_dates())                                                             # Nifty All Expiry Date
# print(get.fno_expiry_dates("TCS"))                                                        # TCS All Expiry Date

# print(get.fno_expiry_dates("NIFTY", "Current"))                                           # Nifty Current Expiry Date only → "28-10-2025"
# print(get.fno_expiry_dates("NIFTY", "Next Week"))                                         # Nifty Next Week Expiry Date only → "04-11-2025"
# print(get.fno_expiry_dates("NIFTY", "Month"))                                             # Nifty Month Expiry Date only → "25-11-2025"
# print(get.fno_expiry_dates("NIFTY", "All"))                                               # → ["28-10-2025", "04-11-2025", "25-11-2025"]

# print(get.fno_expiry_dates("TCS", "Current"))                                             # TCS Current Expiry Date only
# print(get.fno_expiry_dates("TCS", "Month"))                                               # TCS Next Month Expiry Date only


# # 🔹 Option Chain
# print(get.fno_live_option_chain("RELIANCE"))                                              # Option chain for a stock symbol
# print(get.fno_live_option_chain("NIFTY"))                                                 # Option chain for an index
# print(get.fno_live_option_chain("RELIANCE", expiry_date="27-Jan-2026"))                   # Option chain with specific expiry
# print(get.fno_live_option_chain("RELIANCE", oi_mode="compact"))                           # Compact option chain data

# # 🔹 Active Contracts
# print(get.fno_live_active_contracts("NIFTY"))                                             # Active index option contracts
# print(get.fno_live_active_contracts("NIFTY", expiry_date="09-12-2025"))                   # Active index contracts with expiry

# print(get.fno_live_active_contracts("RELIANCE"))                                          # Active stock option contracts
# print(get.fno_live_active_contracts("RELIANCE", expiry_date="30-10-2025"))                # Active stock contracts with expiry


# #---------------------------------------------------------- CM EOD Data ----------------------------------------------------------

# # 🔹 FII/DII Activity
# print(get.cm_eod_fii_dii_activity())                                                      # Latest FII/DII trading activity
# print(get.cm_eod_fii_dii_activity("Nse"))                                                 # Latest FII/DII trading activity in NSE

# # 🔹 Market Activity Report
# print(get.cm_eod_market_activity_report("17-10-25"))                                      # Market activity for specific date

# # 🔹 Bhavcopy with Delivery
# print(get.cm_eod_bhavcopy_with_delivery("17-10-2025"))                                    # Full bhavcopy with delivery data

# # 🔹 Equity Bhavcopy
# print(get.cm_eod_equity_bhavcopy("17-10-2025"))                                           # Equity-only bhavcopy

# # 🔹 52-Week High/Low
# print(get.cm_eod_52_week_high_low("17-10-2025"))                                          # 52-week high/low for date

# # 🔹 Bulk Deals (Latest)
# print(get.cm_eod_bulk_deal())                                                             # Latest bulk deals

# # 🔹 Block Deals (Latest)
# print(get.cm_eod_block_deal())                                                            # Latest block deals

# # 🔹 Short Selling
# print(get.cm_eod_shortselling("17-10-2025"))                                              # Short selling for date

# # 🔹 Surveillance Indicator
# print(get.cm_eod_surveillance_indicator("17-10-25"))                                      # Surveillance for date (yy format)

# # 🔹 Series Change
# print(get.cm_eod_series_change())                                                         # Latest series changes

# # 🔹 Equity Band Changes
# print(get.cm_eod_eq_band_changes("17-10-2025"))                                           # Band changes for date

# # 🔹 Equity Price Band(EOD)
# print(get.cm_eod_eq_price_band("17-10-2025"))                                             # Price bands for date

# # 🔹 Equity Price Band(Historical)
# print(get.cm_hist_eq_price_band())                                                        # today date data for all symbol
# print(get.cm_hist_eq_price_band("1W"))                                                    # 1D, 1W, 1M, 3M, 6M, 1Y
# print(get.cm_hist_eq_price_band("01-10-2025"))                                            # From Date given auto To date (today Date)
# print(get.cm_hist_eq_price_band("15-10-2025", "17-10-2025"))                              # Date range
# print(get.cm_hist_eq_price_band("WEWIN"))                                                 # Bulk deals for symbol
# print(get.cm_hist_eq_price_band("WEWIN", "1Y"))                                           # 1Y for symbol    1D, 1W, 1M, 3M, 6M, 1Y
# print(get.cm_hist_eq_price_band("DSSL", "01-10-2025"))                                    # From Date given auto To date (today Date) for symbol
# print(get.cm_hist_eq_price_band("DSSL", "01-10-2025", "17-10-2025"))                      # Date range for symbol

# # 🔹 PE Ratio
# print(get.cm_eod_pe_ratio("17-10-25"))                                                    # PE ratios for date (yy format)

# # 🔹 Market Cap
# print(get.cm_eod_mcap("17-10-25"))                                                        # Market cap for date (yy format)

# # 🔹 Equity Name Change
# print(get.cm_eod_eq_name_change())                                                        # Latest name changes

# # 🔹 Equity Symbol Change
# print(get.cm_eod_eq_symbol_change())                                                      # Latest symbol changes

# # 🔹 Historical Security Data
# print(get.cm_hist_security_wise_data("RELIANCE"))                                         # 1Y data for symbol
# print(get.cm_hist_security_wise_data("RELIANCE", "1Y"))                                   # 1Y for symbol    1D, 1W, 1M, 3M, 6M, 1Y
# print(get.cm_hist_security_wise_data("RELIANCE", "01-10-2025", "17-10-2025"))             # Date range for symbol

# # 🔹 Historical Bulk Deals
# print(get.cm_hist_bulk_deals())                                                           # today date data for all symbol
# print(get.cm_hist_bulk_deals("1W"))                                                       # 1D, 1W, 1M, 3M, 6M, 1Y
# print(get.cm_hist_bulk_deals("01-10-2025"))                                               # From Date given auto To date (today Date)
# print(get.cm_hist_bulk_deals("15-10-2025", "17-10-2025"))                                 # Date range
# print(get.cm_hist_bulk_deals("RELIANCE"))                                                 # Bulk deals for symbol
# print(get.cm_hist_bulk_deals("DSSL", "1Y"))                                               # 1Y for symbol    1D, 1W, 1M, 3M, 6M, 1Y
# print(get.cm_hist_bulk_deals("DSSL", "01-10-2025"))                                       # From Date given auto To date (today Date) for symbol
# print(get.cm_hist_bulk_deals("DSSL", "01-10-2025", "17-10-2025"))                         # Date range for symbol

# # 🔹 Historical Block Deals
# print(get.cm_hist_block_deals())                                                          # today date data for all symbol
# print(get.cm_hist_block_deals("1W"))                                                      # 1D, 1W, 1M, 3M, 6M, 1Y
# print(get.cm_hist_block_deals("01-10-2025"))                                              # From Date given auto To date (today Date)
# print(get.cm_hist_block_deals("15-10-2025", "17-10-2025"))                                # Date range
# print(get.cm_hist_block_deals("RELIANCE"))                                                # Bulk deals for symbol
# print(get.cm_hist_block_deals("DSSL", "1Y"))                                              # 1Y for symbol    1D, 1W, 1M, 3M, 6M, 1Y
# print(get.cm_hist_block_deals("DSSL", "01-10-2025"))                                      # From Date given auto To date (today Date) for symbol
# print(get.cm_hist_block_deals("DSSL", "01-10-2025", "17-10-2025"))                        # Date range for symbol


# # 🔹 Historical Short Selling
# print(get.cm_hist_short_selling())                                                        # today date data for all symbol
# print(get.cm_hist_short_selling("1W"))                                                    # 1D, 1W, 1M, 3M, 6M, 1Y
# print(get.cm_hist_short_selling("01-10-2025"))                                            # From Date given auto To date (today Date)
# print(get.cm_hist_short_selling("15-10-2025", "17-10-2025"))                              # Date range
# print(get.cm_hist_short_selling("RELIANCE"))                                              # Bulk deals for symbol
# print(get.cm_hist_short_selling("DSSL", "1Y"))                                            # 1Y for symbol    1D, 1W, 1M, 3M, 6M, 1Y
# print(get.cm_hist_short_selling("DSSL", "01-10-2025"))                                    # From Date given auto To date (today Date) for symbol
# print(get.cm_hist_short_selling("DSSL", "01-10-2025", "17-10-2025"))                      # Date range for symbol


# # 🔹 Business Growth Data
# print(get.cm_dmy_biz_growth())                                                            # Current month daily
# print(get.cm_dmy_biz_growth("monthly"))                                                   # Current FY monthly
# print(get.cm_dmy_biz_growth("yearly"))                                                    # All yearly
# print(get.cm_dmy_biz_growth("daily", "OCT", 2025))                                        # Oct 2025 daily
# print(get.cm_dmy_biz_growth("monthly", 2025))                                             # FY 2025 monthly

# # 🔹 Monthly Settlement Report
# print(get.cm_monthly_settlement_report())                                                 # Current FY
# print(get.cm_monthly_settlement_report("1Y"))                                             # Last 1 FY
# print(get.cm_monthly_settlement_report("2024", 2026))                                     # FY 2024-25 to 2025-26
# print(get.cm_monthly_settlement_report("3Y"))                                             # Last 3 FYs

# # 🔹 Monthly Most Active Equity
# print(get.cm_monthly_most_active_equity())                                                # Latest monthly most active

# # 🔹 Advances/Declines
# print(get.historical_advances_decline())                                                  # Previous month (Month_wise)
# print(get.historical_advances_decline("2025"))                                            # 2025 Month_wise
# print(get.historical_advances_decline("Day_wise", "OCT", 2025))                           # Oct 2025 Day_wise
# print(get.historical_advances_decline("Month_wise", 2024))                                # 2024 Month_wise


# #---------------------------------------------------------- FnO EOD Data ----------------------------------------------------------

# # 🔹 F&O Bhavcopy
# print(get.fno_eod_bhav_copy("17-10-2025"))                                                # F&O bhavcopy for a specific trade date (DD-MM-YYYY)

# # 🔹 FII Stats
# print(get.fno_eod_fii_stats("17-10-2025"))                                                # FII statistics for a specific trade date (DD-MM-YYYY)

# # 🔹 Top 10 Futures
# print(get.fno_eod_top10_fut("17-10-2025"))                                                # Top 10 futures contracts (DD-MM-YYYY)

# # 🔹 Top 20 Options
# print(get.fno_eod_top20_opt("17-10-2025"))                                                # Top 20 options contracts (DD-MM-YYYY)

# # 🔹 Security Ban
# print(get.fno_eod_sec_ban("17-10-2025"))                                                  # Securities in ban period (DD-MM-YYYY)

# # 🔹 MWPL (Market Wide Position Limit)
# print(get.fno_eod_mwpl_3("17-10-2025"))                                                   # MWPL data for a specific trade date (DD-MM-YYYY)

# # 🔹 Combined Open Interest
# print(get.fno_eod_combine_oi("17-10-2025"))                                               # Combined OI data (DD-MM-YYYY)

# # 🔹 Participant-Wise Open Interest
# print(get.fno_eod_participant_wise_oi("17-10-2025"))                                      # Participant-wise OI data (DD-MM-YYYY)

# # 🔹 Participant-Wise Volume
# print(get.fno_eod_participant_wise_vol("17-10-2025"))                                     # Participant-wise volume data (DD-MM-YYYY)

# # 🔹  Historical Futures
# print(get.future_price_volume_data("NIFTY", "Index", "OCT-25", "01-10-2025", "17-10-2025"))
# print(get.future_price_volume_data("ITC", "Stock Futures", "OCT-25", "04-10-2025"))
# print(get.future_price_volume_data("BANKNIFTY", "Index Futures", "3M"))
# print(get.future_price_volume_data("NIFTY", "Index Futures", "NOV-24"))

# # 🔹  Historical Options
# print(get.option_price_volume_data("NIFTY", "Index", "01-10-2025", "17-10-2025", expiry= "20-10-2025"))
# print(get.option_price_volume_data("ITC", "Stock Options","CE", "01-10-2025", "17-10-2025", expiry= "28-10-2025"))
# print(get.option_price_volume_data("BANKNIFTY", "Index Options","47000", "01-10-2025", "17-10-2025", expiry= "28-10-2025"))
# print(get.option_price_volume_data("ITC", "Stock Options", "04-10-2025", expiry= "28-10-2025"))
# print(get.option_price_volume_data("BANKNIFTY", "Index Options", "3M"))
# print(get.option_price_volume_data("NIFTY", "Index Options","PE", "01-10-2025", expiry= "28-10-2025"))

# # 🔹 F&O Lot Size
# print(get.fno_eom_lot_size())                                                             # Latest F&O lot sizes
# print(get.fno_eom_lot_size("TCS"))                                                        # F&O lot sizes for symbol 

# # 🔹 DMY Business Growth
# print(get.fno_dmy_biz_growth())                                                           # Monthly F&O business growth (default: current year)
# print(get.fno_dmy_biz_growth("yearly"))                                                   # Yearly F&O data
# print(get.fno_dmy_biz_growth("daily", month="OCT", year=2025))                            # Daily F&O data for specific month/year

# # 🔹 Monthly Settlement Report
# print(get.fno_monthly_settlement_report())                                                # Current FY F&O settlement stats
# print(get.fno_monthly_settlement_report("2024", "2025"))                                  # Specific FY range
# print(get.fno_monthly_settlement_report("2Y"))                                            # Last 2 FYs


# #---------------------------------------------------------- SEBI Data ----------------------------------------------------------

# # 🔹 SEBI Circulars
# print(get.sebi_circulars())                                                               # Default: last 1 week
# print(get.sebi_circulars("01-10-2025", "10-10-2025"))                                     # Specific date range (DD-MM-YYYY)
# print(get.sebi_circulars("01-10-2025"))                                                   # From date to today
# print(get.sebi_circulars("1M"))                                                           # 1W, 2W, 3W, 1M, 2M, 3M, 6M, 1Y , 2Y

# # 🔹 SEBI Data (Paged Circulars)
# print(get.sebi_data())                                                                    # Fetch latest SEBI circulars (default: 1 page)






# #---------------------------------------------------------- Money control ----------------------------------------------------------

# # 🔹 Advances/Declines data
# print(mc.fetch_adv_dec("NIFTY 50"))                                                       # Advances/Declines data
# print(mc.fetch_adv_dec("NIFTY 500"))                                                      # Advances/Declines data
