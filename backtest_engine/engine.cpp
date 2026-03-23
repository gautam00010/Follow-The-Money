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

int main() {
    cout << "Initializing AlphaWeave Quantitative Backtester..." << endl;

    string input_path = "data/processed/signals.csv"; 
    string report_path = "REPORT.md";
    ifstream file(input_path);
    if (!file.is_open()) {
        cerr << "CRITICAL ERROR: Cannot open " << input_path << ". Did the Python fusion step run?" << endl;
        return 1;
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
        
        getline(s, word, ',');
        row.zscore = stod(word);
        
        history.push_back(row);
    }
    file.close();

    if (history.empty()) {
        cerr << "CRITICAL ERROR: No data found in signals.csv" << endl;
        return 1;
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

    // --- CALCULATE RISK METRICS ---
    double cumulative_return = ((current_equity - starting_equity) / starting_equity) * 100.0;
    
    double mean_return = 0.0;
    for (double r : daily_returns) mean_return += r;
    mean_return /= daily_returns.size();
    
    double variance = 0.0;
    for (double r : daily_returns) variance += pow(r - mean_return, 2);
    variance /= (daily_returns.size() - 1);
    
    // Annualized Sharpe Ratio (assuming 252 trading days)
    double sharpe_ratio = 0.0;
    if (variance > 0) {
        sharpe_ratio = (mean_return / sqrt(variance)) * sqrt(252);
    }

    // --- GENERATE INSTITUTIONAL REPORT ---
    ofstream report("../REPORT.md");
    report << "# AlphaWeave Quantitative Strategy Report\n\n";
    report << "## Backtest Results\n";
    report << "* **Total Trading Days Simulated:** " << history.size() << "\n";
    report << "* **Starting Capital:** $" << fixed << setprecision(2) << starting_equity << "\n";
    report << "* **Ending Capital:** $" << fixed << setprecision(2) << current_equity << "\n";
    report << "* **Cumulative Return:** " << fixed << setprecision(2) << cumulative_return << "%\n";
    report << "* **Maximum Drawdown:** " << fixed << setprecision(2) << (max_drawdown * 100.0) << "%\n";
    report << "* **Annualized Sharpe Ratio:** " << fixed << setprecision(2) << sharpe_ratio << "\n\n";
    report << "## Signal Methodology\n";
    report << "This model systematically enters the market when alternative labor data (salary momentum) exhibits a Z-Score greater than " << ENTRY_THRESHOLD << " and exits to cash when momentum falls below " << EXIT_THRESHOLD << ".";
    report.close();

    cout << "SUCCESS: Backtest complete. Risk metrics saved to REPORT.md" << endl;
    return 0;
}
