#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cmath>
#include <iomanip>
#include <algorithm>

using namespace std;

// Data structure to hold our daily signal
struct DailyData {
    string date;
    double close;
    double zscore;
};

struct PerformanceSummary {
    string ticker;
    double cumulative_return;
    double sharpe_ratio;
    double max_drawdown_pct;
    string recommendation;
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
    report << "| Ticker | Cumulative Return | Sharpe Ratio | Max Drawdown | Recommendation |\n";
    report << "| --- | --- | --- | --- | --- |\n";
    for (const auto& row : summaries) {
        report << "| "
               << row.ticker << " | "
               << row.cumulative_return << "% | "
               << row.sharpe_ratio << " | "
               << row.max_drawdown_pct << "% | "
               << row.recommendation << " |\n";
    }
    report << "\n## Signal Methodology\n";
    report << "This model systematically enters the market when alternative labor data (salary momentum) exhibits a Z-Score greater than 2.0 and exits to cash when momentum falls below -1.0.";
    report.close();
}

int main() {
    cout << "Initializing AlphaWeave Quantitative Backtester..." << endl;

    try {
        string input_path = "data/processed/signals.csv"; 
        string report_path = "REPORT.md";
        ifstream file(input_path);
        if (!file.is_open()) {
            throw runtime_error("CRITICAL ERROR: Cannot open " + input_path + ". Did the Python fusion step run?");
        }

        vector<DailyData> history;
        string line, word;
        
        // Skip the CSV header
        getline(file, line);
        
        while (getline(file, line)) {
            stringstream s(line);
            DailyData row;
            getline(s, row.date, ',');
            
            getline(s, word, ',');
            row.close = stod(word);
            if (isnan(row.close) || row.close == 0.0) {
                throw runtime_error("DATA QUALITY ERROR: Price column contains NaN or zero at date " + row.date);
            }
            
            getline(s, word, ',');
            row.zscore = stod(word);
            
            history.push_back(row);
        }
        file.close();

        if (history.empty()) {
            throw runtime_error("CRITICAL ERROR: No data found in signals.csv");
        }

        // --- SYSTEMATIC TRADING LOGIC ---
        // If Salary Z-Score > 2.0 (Massive Hiring/Salary Spike) -> Buy the ETF
        // If Salary Z-Score < -1.0 (Contraction) -> Sell/Move to Cash
        
        const double ENTRY_THRESHOLD = 2.0;
        const double EXIT_THRESHOLD = -1.0;
        
        double current_position = 0.0; // 0.0 = Cash, 1.0 = Invested
        double starting_equity = 100000.0; // Start with $100k
        double current_equity = starting_equity;
        double peak_equity = starting_equity;
        double max_drawdown = 0.0;
        
        vector<double> daily_returns;

        for (size_t i = 1; i < history.size(); ++i) {
            // Calculate how much the stock moved today
            double stock_return = (history[i].close - history[i-1].close) / history[i-1].close;
            
            // Calculate our portfolio return based on yesterday's position (No lookahead bias!)
            double strategy_return = stock_return * current_position;
            current_equity *= (1.0 + strategy_return);
            
            daily_returns.push_back(strategy_return);

            // Update Maximum Drawdown
            if (current_equity > peak_equity) {
                peak_equity = current_equity;
            }
            double drawdown = (peak_equity - current_equity) / peak_equity;
            if (drawdown > max_drawdown) {
                max_drawdown = drawdown;
            }

            // --- UPDATE POSITION FOR TOMORROW ---
            if (history[i].zscore > ENTRY_THRESHOLD) {
                current_position = 1.0; // Go Long
            } else if (history[i].zscore < EXIT_THRESHOLD) {
                current_position = 0.0; // Move to Cash
            }
        }

        if (daily_returns.empty()) {
            throw runtime_error("INSUFFICIENT DATA: Unable to compute returns from signals.csv");
        }

        // --- CALCULATE RISK METRICS ---
        double cumulative_return = ((current_equity - starting_equity) / starting_equity) * 100.0;
        
        double mean_return = 0.0;
        for (double r : daily_returns) mean_return += r;
        mean_return /= daily_returns.size();
        
        double variance = 0.0;
        for (double r : daily_returns) variance += pow(r - mean_return, 2);
        if (daily_returns.size() > 1) {
            variance /= (daily_returns.size() - 1);
        }
        
        // Annualized Sharpe Ratio (assuming 252 trading days)
        double sharpe_ratio = 0.0;
        if (variance > 0) {
            sharpe_ratio = (mean_return / sqrt(variance)) * sqrt(252);
        }

        vector<PerformanceSummary> summaries;
        PerformanceSummary row{
            "UNIVERSE",
            cumulative_return,
            sharpe_ratio,
            max_drawdown * 100.0,
            determine_recommendation(sharpe_ratio, cumulative_return)
        };
        summaries.push_back(row);

        write_report(summaries, history.size(), starting_equity, current_equity, report_path);

        cout << "SUCCESS: Backtest complete. Risk metrics saved to " << report_path << endl;
        return 0;
    } catch (const exception& ex) {
        cerr << ex.what() << endl;
        return 1;
    }
}
