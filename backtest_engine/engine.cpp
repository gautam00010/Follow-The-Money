#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

struct SignalRow {
  std::string date;
  double close;
  double job_zscore;
};

enum class Position { SHORT = -1, FLAT = 0, LONG = 1 };

int position_multiplier(Position p) {
  switch (p) {
  case Position::LONG:
    return 1;
  case Position::SHORT:
    return -1;
  case Position::FLAT:
  default:
    return 0;
  }
}

std::vector<std::string> split(const std::string &line, char delim = ',') {
  std::vector<std::string> parts;
  std::stringstream ss(line);
  std::string item;
  while (std::getline(ss, item, delim)) {
    parts.push_back(item);
  }
  return parts;
}

SignalRow parse_row(const std::vector<std::string> &headers,
                    const std::vector<std::string> &fields) {
  auto find_idx = [&](const std::string &name) -> std::size_t {
    for (std::size_t i = 0; i < headers.size(); ++i) {
      if (headers[i] == name)
        return i;
    }
    throw std::runtime_error("Column missing: " + name);
  };

  std::size_t date_idx = find_idx("date");
  std::size_t close_idx = find_idx("close");
  std::size_t z_idx = find_idx("job_zscore");

  if (fields.size() <= std::max({date_idx, close_idx, z_idx})) {
    throw std::runtime_error("Row has insufficient columns");
  }

  SignalRow row;
  row.date = fields[date_idx];
  row.close = std::stod(fields[close_idx]);
  row.job_zscore = std::stod(fields[z_idx]);
  return row;
}

std::vector<SignalRow> load_signals(const std::string &path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    throw std::runtime_error("Unable to open signals file: " + path);
  }

  std::string header_line;
  if (!std::getline(file, header_line)) {
    throw std::runtime_error("Signals file is empty");
  }
  auto headers = split(header_line);

  std::vector<SignalRow> rows;
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) {
      continue;
    }
    auto fields = split(line);
    rows.push_back(parse_row(headers, fields));
  }

  if (rows.size() < 2) {
    throw std::runtime_error("Insufficient observations for backtest.");
  }
  return rows;
}

struct Metrics {
  double cumulative_return;
  double max_drawdown;
  double sharpe_ratio;
};

Metrics backtest(const std::vector<SignalRow> &rows) {
  constexpr double TRADING_DAYS = 252.0;
  constexpr double EXPANSION_THRESHOLD = 2.0;
  constexpr double CONTRACTION_THRESHOLD = -1.0;
  double equity = 1.0;
  double peak = 1.0;
  double max_dd = 0.0;

  std::vector<double> strategy_returns;
  Position position = Position::FLAT;

  for (std::size_t i = 1; i < rows.size(); ++i) {
    const auto &prev = rows[i - 1];
    const auto &curr = rows[i];

    if (prev.job_zscore > EXPANSION_THRESHOLD) {
      position = Position::LONG;
    } else if (prev.job_zscore < CONTRACTION_THRESHOLD) {
      position = Position::SHORT;
    } else {
      position = Position::FLAT;
    }

    double daily_return = (curr.close / prev.close) - 1.0;
    double strat_return = static_cast<double>(position_multiplier(position)) * daily_return;
    strategy_returns.push_back(strat_return);

    equity *= (1.0 + strat_return);
    if (equity <= 0.0) {
      throw std::runtime_error(
          "Equity fell to a non-positive level on " + curr.date +
          ". Inspect signals.csv for gaps/outliers and reconsider threshold parameters.");
    }
    if (equity > peak) {
      peak = equity;
    }
    double drawdown = (equity / peak) - 1.0;
    if (drawdown < max_dd) {
      max_dd = drawdown;
    }
  }

  // Compute Sharpe ratio with daily returns annualized to 252 trading days.
  const auto n = strategy_returns.size();
  double mean = 0.0;
  for (double r : strategy_returns) {
    mean += r;
  }
  mean /= static_cast<double>(n);

  double variance = 0.0;
  for (double r : strategy_returns) {
    variance += (r - mean) * (r - mean);
  }
  if (n > 1) {
    variance /= static_cast<double>(n - 1);
  }
  double sharpe =
      variance > 0 ? (mean / std::sqrt(variance)) * std::sqrt(TRADING_DAYS) : 0.0;

  Metrics metrics;
  metrics.cumulative_return = equity - 1.0;
  metrics.max_drawdown = max_dd;
  metrics.sharpe_ratio = sharpe;
  return metrics;
}

void write_report(const Metrics &m, const std::string &path) {
  std::ofstream out(path);
  if (!out.is_open()) {
    throw std::runtime_error("Unable to write report to: " + path);
  }

  out << "# AlphaWeave MVP Backtest Report\n\n";
  out << "Signals: Buy XLK when job posting z-score exceeds 2.0 (expansion), "
         "exit/reduce when below -1.0 (contraction).\n\n";
  out << "| Metric | Value |\n";
  out << "| --- | --- |\n";
  out << "| Cumulative Return | " << std::fixed << std::setprecision(4)
      << m.cumulative_return * 100 << "% |\n";
  out << "| Maximum Drawdown | " << m.max_drawdown * 100 << "% |\n";
  out << "| Sharpe Ratio | " << m.sharpe_ratio << " |\n";
}

int main() {
  try {
    auto signals = load_signals("data/processed/signals.csv");
    Metrics metrics = backtest(signals);
    write_report(metrics, "REPORT.md");
    std::cout << "Backtest complete. Metrics written to REPORT.md\n";
  } catch (const std::exception &ex) {
    std::cerr << "Backtest failed: " << ex.what() << std::endl;
    return 1;
  }
  return 0;
}
