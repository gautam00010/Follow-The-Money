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

BacktestResult run_backtest(const string& ticker, const vector<PricePoint>& history) {
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
        string signal_path = "../data/processed/signals.csv";
        string price_path = "../data/raw/universe_prices.csv";
        string report_path = "../REPORT.md";
        ifstream signal_file(signal_path);
        if (!signal_file.is_open()) {
            throw runtime_error("CRITICAL ERROR: Cannot open " + signal_path + ". Did the Python fusion step run?");
        }

        unordered_map<string, double> job_zscores;
        string line, word;

        getline(signal_file, line);
        while (getline(signal_file, line)) {
            stringstream s(line);
            string date;
            getline(s, date, ','); // date
            getline(s, word, ','); // close (ignored)
            getline(s, word, ','); // job_zscore
            if (!date.empty() && !word.empty()) {
                job_zscores[date] = stod(word);
            }
        }
        signal_file.close();

        ifstream price_file(price_path);
        if (!price_file.is_open()) {
            throw runtime_error("CRITICAL ERROR: Cannot open " + price_path + ". Did the ingestion step run?");
        }

        map<string, vector<PricePoint>> price_history_by_symbol;

        getline(price_file, line); // header
        size_t total_unique_days = 0;
        unordered_map<string, bool> seen_dates;
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
            auto it = job_zscores.find(date);
            if (it != job_zscores.end()) {
                signal_value = it->second;
            }

            price_history_by_symbol[symbol].push_back({date, close, signal_value});

            if (!seen_dates.count(date)) {
                seen_dates[date] = true;
                ++total_unique_days;
            }
        }
        price_file.close();

        if (price_history_by_symbol.empty()) {
            throw runtime_error("CRITICAL ERROR: No data found in universe_prices.csv");
        }

        vector<PerformanceSummary> summaries;
        map<string, vector<double>> aggregate_returns;
        double total_starting_equity = 0.0;
        const double STARTING_EQUITY_PER_TICKER = 100000.0;

        for (auto& kv : price_history_by_symbol) {
            auto& history = kv.second;
            sort(history.begin(), history.end(), [](const PricePoint& a, const PricePoint& b) {
                return a.date < b.date;
            });

            if (history.size() < 2) {
                cerr << "WARNING: Skipping " << kv.first << " due to insufficient history." << endl;
                continue;
            }

            BacktestResult result = run_backtest(kv.first, history);
            summaries.push_back(result.summary);
            total_starting_equity += result.starting_equity;

            for (const auto& dr : result.dated_returns) {
                aggregate_returns[dr.first].push_back(dr.second);
            }
        }

        if (summaries.empty()) {
            throw runtime_error("CRITICAL ERROR: No valid ticker histories to backtest.");
        }

        vector<pair<string, double>> averaged_returns;
        for (const auto& kv : aggregate_returns) {
            double avg_return = accumulate(kv.second.begin(), kv.second.end(), 0.0) / kv.second.size();
            averaged_returns.push_back({kv.first, avg_return});
        }
        sort(averaged_returns.begin(), averaged_returns.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });

        vector<double> total_daily_returns;
        double aggregate_equity = total_starting_equity;
        double aggregate_peak = total_starting_equity;
        double aggregate_max_drawdown = 0.0;

        for (const auto& ar : averaged_returns) {
            aggregate_equity *= (1.0 + ar.second);
            total_daily_returns.push_back(ar.second);
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
        for (const auto& row : summaries) {
            benchmark_equity += STARTING_EQUITY_PER_TICKER * (1.0 + row.benchmark_return / 100.0);
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

        write_report(summaries, total_unique_days, total_starting_equity, aggregate_equity, report_path);

        cout << "SUCCESS: Backtest complete. Risk metrics saved to " << report_path << endl;
        return 0;
    } catch (const exception& ex) {
        cerr << ex.what() << endl;
        return 1;
    }
}
