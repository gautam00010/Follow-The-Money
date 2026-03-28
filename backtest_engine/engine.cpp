#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cmath>
#include <iomanip>
#include <algorithm>
#include <map>
#include <unordered_map>
#include <numeric>
#include <filesystem>
#include <cstdlib>

using namespace std;

struct PerformanceSummary {
    string ticker;
    double cumulative_return;
    double sharpe_ratio;
    double max_drawdown_pct;
    double benchmark_return;
    string recommendation;
};

struct PricePoint {
    string date;
    double close;
    double signal;
};

struct BacktestResult {
    PerformanceSummary summary;
    double starting_equity;
    double ending_equity;
    vector<pair<string, double>> dated_returns;
};

string determine_recommendation(double sharpe, double cumulative_return) {
    if (sharpe > 1.0 && cumulative_return > 0.0) {
        return "STRONG BUY";
    }
    if (sharpe > 0.5) {
        return "BUY";
    }
    return "HOLD";
}

void write_report(const vector<PerformanceSummary>& summaries,
                  size_t total_days,
                  double starting_equity,
                  double ending_equity,
                  const string& report_path) {
    ofstream report(report_path);
    report << fixed << setprecision(2);
    report << "# AlphaWeave Quantitative Strategy Report\n\n";
    report << "## Backtest Results\n";
    report << "* **Total Trading Days Simulated:** " << total_days << "\n";
    report << "* **Starting Capital:** $" << starting_equity << "\n";
    report << "* **Ending Capital:** $" << ending_equity << "\n\n";
    report << "## Performance by Ticker\n";
    report << "| Ticker | Cumulative Return | Sharpe Ratio | Max Drawdown | Benchmark (B&H) | Recommendation |\n";
    report << "| --- | --- | --- | --- | --- | --- |\n";
    for (const auto& row : summaries) {
        report << "| "
               << row.ticker << " | "
               << row.cumulative_return << "% | "
               << row.sharpe_ratio << " | "
               << row.max_drawdown_pct << "% | "
               << row.benchmark_return << "% | "
               << row.recommendation << " |\n";
    }
    report << "\n## Signal Methodology\n";
    report << "This model systematically enters the market when alternative labor data (salary momentum) exhibits a Z-Score greater than 2.0 and exits to cash when momentum falls below -1.0.";
    report.close();
}

// Run a single-ticker backtest using salary z-score thresholds:
// - Enter long when signal > 2.0, exit to cash when signal < -1.0
// - Track equity from a $100k starting balance with no leverage
// - Return cumulative return, Sharpe, max drawdown, benchmark, and dated returns
BacktestResult run_ticker_backtest(const string& ticker, const vector<PricePoint>& history) {
    if (history.size() < 2) {
        throw runtime_error("INSUFFICIENT DATA: Not enough price history for " + ticker);
    }

    const double ENTRY_THRESHOLD = 2.0;
    const double EXIT_THRESHOLD = -1.0;
    const double STARTING_EQUITY = 100000.0;

    double current_position = 0.0; // 0.0 = Cash, 1.0 = Invested
    double current_equity = STARTING_EQUITY;
    double peak_equity = STARTING_EQUITY;
    double max_drawdown = 0.0;

    vector<double> daily_returns;
    vector<pair<string, double>> dated_returns;

    for (size_t i = 1; i < history.size(); ++i) {
        double stock_return = (history[i].close - history[i-1].close) / history[i-1].close;
        double strategy_return = stock_return * current_position;
        current_equity *= (1.0 + strategy_return);

        daily_returns.push_back(strategy_return);
        dated_returns.push_back({history[i].date, strategy_return});

        if (current_equity > peak_equity) {
            peak_equity = current_equity;
        }
        double drawdown = (peak_equity - current_equity) / peak_equity;
        if (drawdown > max_drawdown) {
            max_drawdown = drawdown;
        }

        if (history[i].signal > ENTRY_THRESHOLD) {
            current_position = 1.0; // Go Long
        } else if (history[i].signal < EXIT_THRESHOLD) {
            current_position = 0.0; // Move to Cash
        }
    }

    double cumulative_return = ((current_equity - STARTING_EQUITY) / STARTING_EQUITY) * 100.0;
    double mean_return = accumulate(daily_returns.begin(), daily_returns.end(), 0.0) / daily_returns.size();

    double variance = 0.0;
    for (double r : daily_returns) variance += pow(r - mean_return, 2);
    if (daily_returns.size() > 1) {
        variance /= (daily_returns.size() - 1);
    }

    double sharpe_ratio = 0.0;
    if (variance > 0) {
        sharpe_ratio = (mean_return / sqrt(variance)) * sqrt(252);
    }

    double benchmark_return = ((history.back().close / history.front().close) - 1.0) * 100.0;

    PerformanceSummary summary{
        ticker,
        cumulative_return,
        sharpe_ratio,
        max_drawdown * 100.0,
        benchmark_return,
        determine_recommendation(sharpe_ratio, cumulative_return)
    };

    return BacktestResult{summary, STARTING_EQUITY, current_equity, dated_returns};
}

int main() {
    cout << "Initializing AlphaWeave Quantitative Backtester..." << endl;

    try {
        const char* env_signal = getenv("SIGNALS_CSV_PATH");
        const char* env_prices = getenv("UNIVERSE_PRICES_PATH");
        const char* env_report = getenv("REPORT_PATH");

        auto find_repo_root = []() {
            filesystem::path probe = filesystem::current_path();
            for (int i = 0; i < 5 && !probe.empty(); ++i) {
                if (filesystem::exists(probe / "data") || filesystem::exists(probe / ".git")) {
                    return probe;
                }
                probe = probe.parent_path();
            }
            return filesystem::current_path();
        };

        filesystem::path base_dir = find_repo_root();

        string signal_path = env_signal ? string(env_signal) : (base_dir / "data/processed/signals.csv").string();
        string price_path = env_prices ? string(env_prices) : (base_dir / "data/raw/universe_prices.csv").string();
        string report_path = env_report ? string(env_report) : (base_dir / "REPORT.md").string();

        auto validate_path = [](const string& path, const string& label, const string& env_var) {
            if (path.empty()) {
                throw runtime_error("CRITICAL ERROR: " + label + " path is empty. Set " + env_var + " or run from the repo root.");
            }
            if (!filesystem::exists(path)) {
                throw runtime_error("CRITICAL ERROR: " + label + " not found at " + path);
            }
        };

        validate_path(signal_path, "Signals CSV", "SIGNALS_CSV_PATH");
        validate_path(price_path, "Universe prices CSV", "UNIVERSE_PRICES_PATH");
        filesystem::path report_parent = filesystem::path(report_path).parent_path();
        if (!report_parent.empty() && !filesystem::exists(report_parent)) {
            filesystem::create_directories(report_parent);
        }
        ifstream signal_file(signal_path);
        if (!signal_file.is_open()) {
            throw runtime_error("CRITICAL ERROR: Cannot open " + signal_path + ". Did the Python fusion step run?");
        }

        unordered_map<string, double> salary_zscores;
        string line, word;

        getline(signal_file, line);
        while (getline(signal_file, line)) {
            stringstream s(line);
            string date;
            getline(s, date, ','); // date
            getline(s, word, ','); // close (ignored)
            getline(s, word, ','); // salary_zscore
            if (!date.empty() && !word.empty()) {
                salary_zscores[date] = stod(word);
            }
        }
        signal_file.close();

        ifstream price_file(price_path);
        if (!price_file.is_open()) {
            throw runtime_error("CRITICAL ERROR: Cannot open " + price_path + ". Did the ingestion step run?");
        }

        map<string, vector<PricePoint>> price_history_by_symbol;

        getline(price_file, line); // header
        while (getline(price_file, line)) {
            stringstream s(line);
            string date, symbol, close_str;
            getline(s, date, ',');
            getline(s, symbol, ',');
            getline(s, close_str, ',');

            if (date.empty() || symbol.empty() || close_str.empty()) {
                continue;
            }
            double close = stod(close_str);
            if (isnan(close) || close == 0.0) {
                throw runtime_error("DATA QUALITY ERROR: Price column contains NaN or zero at date " + date + " for " + symbol);
            }

            double signal_value = 0.0;
            auto it = salary_zscores.find(date);
            if (it != salary_zscores.end()) {
                signal_value = it->second;
            }

            price_history_by_symbol[symbol].push_back({date, close, signal_value});
        }
        price_file.close();

        if (price_history_by_symbol.empty()) {
            throw runtime_error("CRITICAL ERROR: No data found in universe_prices.csv");
        }

        vector<PerformanceSummary> summaries;
        map<string, vector<double>> aggregate_returns;
        double total_starting_equity = 0.0;
        size_t max_history_length = 0;
        for (auto& kv : price_history_by_symbol) {
            auto& history = kv.second;
            sort(history.begin(), history.end(), [](const PricePoint& a, const PricePoint& b) {
                return a.date < b.date;
            });

            if (history.size() < 2) {
                cerr << "WARNING: Skipping " << kv.first << " due to insufficient history." << endl;
                continue;
            }

            BacktestResult result = run_ticker_backtest(kv.first, history);
            summaries.push_back(result.summary);
            total_starting_equity += result.starting_equity;
            max_history_length = max(max_history_length, history.size());

            for (const auto& dr : result.dated_returns) {
                aggregate_returns[dr.first].push_back(dr.second);
            }
        }

        if (summaries.empty()) {
            throw runtime_error("CRITICAL ERROR: No valid ticker histories to backtest.");
        }

        vector<double> total_daily_returns;
        double aggregate_equity = total_starting_equity;
        double aggregate_peak = total_starting_equity;
        double aggregate_max_drawdown = 0.0;

        // Aggregate assumes equal weighting by taking the mean return of all tickers that report data on each date.
        // Dates where some tickers are missing simply average over the available names; this intentionally avoids
        // forward-filling or overweighting tickers with longer histories but can introduce survivorship bias.
        for (const auto& kv : aggregate_returns) {
            double avg_return = accumulate(kv.second.begin(), kv.second.end(), 0.0) / kv.second.size();
            aggregate_equity *= (1.0 + avg_return);
            total_daily_returns.push_back(avg_return);
            if (aggregate_equity > aggregate_peak) {
                aggregate_peak = aggregate_equity;
            }
            double drawdown = (aggregate_peak - aggregate_equity) / aggregate_peak;
            if (drawdown > aggregate_max_drawdown) {
                aggregate_max_drawdown = drawdown;
            }
        }

        double mean_total_return = total_daily_returns.empty()
            ? 0.0
            : accumulate(total_daily_returns.begin(), total_daily_returns.end(), 0.0) / total_daily_returns.size();
        double total_variance = 0.0;
        for (double r : total_daily_returns) total_variance += pow(r - mean_total_return, 2);
        if (total_daily_returns.size() > 1) {
            total_variance /= (total_daily_returns.size() - 1);
        }

        double total_sharpe = 0.0;
        if (total_variance > 0) {
            total_sharpe = (mean_total_return / sqrt(total_variance)) * sqrt(252);
        }

        double total_cumulative_return = ((aggregate_equity - total_starting_equity) / total_starting_equity) * 100.0;

        double benchmark_equity = 0.0;
        size_t ticker_count = summaries.size();
        double capital_per_ticker = total_starting_equity / static_cast<double>(ticker_count);
        // Equal-weight the buy-and-hold benchmark across the same tickers to mirror the strategy aggregation.
        for (const auto& row : summaries) {
            benchmark_equity += capital_per_ticker * (1.0 + row.benchmark_return / 100.0);
        }
        double total_benchmark_return = ((benchmark_equity - total_starting_equity) / total_starting_equity) * 100.0;

        PerformanceSummary total_row{
            "TOTAL UNIVERSE",
            total_cumulative_return,
            total_sharpe,
            aggregate_max_drawdown * 100.0,
            total_benchmark_return,
            determine_recommendation(total_sharpe, total_cumulative_return)
        };

        summaries.push_back(total_row);

        write_report(summaries, max_history_length, total_starting_equity, aggregate_equity, report_path);

        cout << "SUCCESS: Backtest complete. Risk metrics saved to " << report_path << endl;
        return 0;
    } catch (const exception& ex) {
        cerr << ex.what() << endl;
        return 1;
    }
}
