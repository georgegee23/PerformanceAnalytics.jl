module PerformanceAnalytics

    export returns_to_prices, pct_change, drawdowns, annual_return, annual_stdev 
    export annual_sharpe_ratio, downside_deviation, sortino_ratio, max_drawdown 
    export down_capture, up_capture, overall_capture, performance_table

########################################## Performance Analytics ###########################################################

using TimeSeries
using DataFrames

#---------------------------------------------------------------------------------

function returns_to_prices(returns::TimeArray, init_value::Int = 1)

    """
    Compute the cumulative product of returns, preserving NaN locations.

    This approach preserves the location of NaN values in the output.
    The cumulative product continues from the last non-NaN value after encountering a NaN.
    It treats the input as returns, so it adds 1 to each value before multiplying (assuming returns are expressed as percentages or decimals).

    Parameters:
    - returns: TimeArray containing return data with possible NaN values

    Returns:
    - TimeArray with cumulative products, preserving NaN locations
    """
    
    values_matrix = values(returns)
    result = similar(values_matrix)
    
    for col in 1:size(values_matrix, 2)
        cumulative = init_value
        for row in 1:size(values_matrix, 1)
            if isnan(values_matrix[row, col])
                result[row, col] = NaN
            else
                cumulative *= (1 + values_matrix[row, col])
                result[row, col] = cumulative
            end
        end
    end
    
    return TimeArray(timestamp(returns), result, colnames(returns))
end


function pct_change(prices::TimeArray, window::Int64=1)


    """

    Calculate the percentage change of prices over a specified window.

    This function computes the percentage change of prices in a TimeArray over a given window size.
    It handles missing values by replacing them with NaN and uses padding to maintain the original
    time series length.

    # Arguments
    - `prices::TimeArray`: A TimeArray containing price data. Can be single or multi-column.
    - `window::Int64`: The size of the window over which to calculate the percentage change.
        Must be at least 1.

    # Returns
    - `TimeArray`: A new TimeArray with the same timestamps and column names as the input,
        but with values representing the percentage changes.

    # Details
    - Missing values in the input are replaced with NaN.
    - The function uses `TimeSeries.lag` with padding to create a lagged version of the prices.
    - Percentage change is calculated as (current_price / lagged_price) - 1.
    - The first `window` number of rows in the result will contain NaN values due to insufficient
        historical data for calculation.

    """



    if window < 1
        throw(ArgumentError("Window size must be at least 1"))
    end

    prices = coalesce.(prices, NaN)
    w_prices_lagged = TimeSeries.lag(prices, window, padding = true)
    ts_matrix = (values(prices) ./ values(w_prices_lagged)) .- 1
    col_names = colnames(prices)
    timestps = timestamp(prices)
    pct_change_ts = TimeArray(timestps, ts_matrix, col_names)

    return pct_change_ts
end



function drawdowns(returns::TimeArray)

    """
    Compute drawdowns for a time array of returns.

    Parameters:
    - returns: TimeArray of asset returns

    Returns:
    - TimeArray of drawdowns
    """

    @assert eltype(values(returns)) <: Real "Input must contain numerical data"
    @assert !any(isnan, values(returns)) "Input contains NaN values"

    prices = returns_to_prices(returns)
    cummax_ta = upto(maximum, prices)
    drawdowns_ta = prices ./ cummax_ta
    drawdowns_ta = drawdowns_ta .- 1
    drawdowns_ta = TimeSeries.rename(drawdowns_ta, colnames(returns))

    return drawdowns_ta
end

function annual_return(returns::TimeArray, periods_per_year::Int)

    """

    Compute annualized return of each column in a TimeArray of returns.

    """
    @assert length(returns) >= 2 "TimeArray must contain at least two data points"

    compounded_growth = returns_to_prices(returns)
    n_periods = size(compounded_growth, 1)
    ann_rets = (last.(eachcol(values(compounded_growth))) .^ (periods_per_year / n_periods)) .- 1

    return ann_rets

end

function annual_stdev(returns::TimeArray, periods_per_year::Int)

    """

    Compute annual standard deviation of each column in a TimeArray of returns.

    """
    
    # Calculate the standard deviation for each column
    std_dev = std.(eachcol(values(returns)))
    
    # Annualize the standard deviation
    annual_std_dev = std_dev .* sqrt(periods_per_year)
    
    # Return as a dictionary for easy column name association
    return annual_std_dev
end

function annual_sharpe_ratio(returns::TimeArray, periods_per_year::Int)

    """
    Compute annual sharpe ratio of each column in a TimeArray of returns.
    """

    return annual_return(returns, periods_per_year) ./ annual_stdev(returns, periods_per_year)
end


function downside_deviation(ta::TimeArray, mar::Number=0; corrected::Bool=true)
    
    """
    Calculate the downside deviation of returns in a TimeArray.

    Parameters:
    - ta: TimeArray containing asset returns
    - mar: Minimum acceptable return (MAR)
    - corrected: Boolean flag indicating whether to use Bessel's correction (default: true)

    Returns:
    - An array of downside deviations.
    """
    
    # Validate input
    @assert all(eltype(values(ta)) <: Real) "All columns must contain numeric data"
    
    # Preallocate results array with the same length as the number of columns in ta
    q_names = colnames(ta)
    results = Vector{Float64}(undef, length(q_names))
    
    for (idx, col) in enumerate(q_names)
        returns = values(ta[col])
        
        # Calculate negative deviations
        negative_returns = returns[returns .< mar]
        
        if isempty(negative_returns)
            results[idx] = 0.0
        else
            # Calculate squared deviations
            squared_deviations = (negative_returns .- mar).^2
            
            n = length(negative_returns)
            denominator = corrected ? n - 1 : n
            
            # Calculate downside variance and then the standard deviation
            downside_var = sum(squared_deviations) / denominator
            results[idx] = sqrt(downside_var)
        end
    end
    
    return results
end

function sortino_ratio(returns::TimeArray, mar::Number; corrected::Bool=true)
    """
    Compute Sortino ratio of each column in a TimeArray of returns.

    Parameters:
    - returns: TimeArray containing asset returns
    - mar: Minimum acceptable return (MAR)
    - corrected: Boolean flag indicating whether to use Bessel's correction (default: true)

    Returns:
    - An array of Sortino ratios for each column.
    """
    
    # Calculate downside deviation
    down_dev = downside_deviation(returns, mar; corrected=corrected)
    
    # Calculate mean returns for each column
    mean_rets = mean(values(returns), dims=1) |> vec  # Get mean returns as a vector
    
    # Avoid division by zero
    sortino_ratios = [down_dev[i] != 0 ? mean_rets[i] / down_dev[i] : NaN for i in 1:length(down_dev)]
    
    return sortino_ratios
end


function max_drawdown(returns::TimeArray)
    """
    Compute the maximum drawdown for each column in a TimeArray of returns.

    Parameters:
    - returns: TimeArray containing asset returns.

    Returns:
    - An array of maximum drawdowns for each column.
    """
    
    # Calculate drawdowns
    dds = drawdowns(returns)
    
    # Compute maximum drawdown for each column
    max_dds = minimum.(eachcol(values(dds)))  # Get minimum drawdown (which is the max drawdown)
    
    return max_dds * -1  # Return as positive values
end


function down_capture(returns::TimeArray, benchmark_returns::TimeArray, thresh_value::Number = 0)
    """
    Compute down capture for each column in a TimeArray of returns.

    Parameters:
    - returns: TimeArray containing portfolio returns
    - benchmark_returns: TimeArray containing benchmark returns
    - thresh_value: Threshold value to determine down markets (default: 0)

    Returns:
    - Vector of down capture ratios for each column in the TimeArray
    """

    # Ensure both TimeArrays have the same timestamps
    if timestamp(returns) != timestamp(benchmark_returns)
        error("The timestamps of returns and benchmark_returns must match.")
    end

    # Identify periods where benchmark is underperforming (down market)
    down_market = values(benchmark_returns) .< thresh_value

    # Filter returns and benchmark returns for down market periods
    portfolio_down = returns[down_market]
    benchmark_down = benchmark_returns[down_market]  # Assuming benchmark data is in a column named 'Benchmark'

    # Check if there are any down markets to analyze
    if isempty(portfolio_down) || isempty(benchmark_down)
        return fill(NaN, length(colnames(returns)))  # Return NaNs if no down capture can be calculated
    end

    # Calculate down capture ratio for each column
    dc_ratio = Vector{Float64}(undef, length(colnames(returns)))
    for (idx, col) in enumerate(colnames(returns))
        avg_portfolio_down = mean(values(portfolio_down[col]))
        avg_benchmark_down = mean(values(benchmark_down))
        if avg_benchmark_down == 0
            dc_ratio[idx] = NaN  # Avoid division by zero
        else
            dc_ratio[idx] = avg_portfolio_down / avg_benchmark_down
        end
    end

    return dc_ratio
end


function up_capture(returns::TimeArray, benchmark_returns::TimeArray, thresh_value::Number = 0)
    """
    Compute up capture for each column in a TimeArray of returns.

    Parameters:
    - returns: TimeArray containing portfolio returns
    - benchmark_returns: TimeArray containing benchmark returns
    - thresh_value: Threshold value to determine up markets (default: 0)

    Returns:
    - Vector of up capture ratios for each column in the TimeArray
    """

    # Ensure both TimeArrays have the same timestamps
    if timestamp(returns) != timestamp(benchmark_returns)
        error("The timestamps of returns and benchmark_returns must match.")
    end

    # Identify periods where benchmark is underperforming (up market)
    up_market = values(benchmark_returns) .> thresh_value

    # Filter returns and benchmark returns for up market periods
    portfolio_up = returns[up_market]
    benchmark_up = benchmark_returns[up_market] 

    # Check if there are any up markets to analyze
    if isempty(portfolio_up) || isempty(benchmark_up)
        return fill(NaN, length(colnames(returns)))  # Return NaNs if no down capture can be calculated
    end

    # Calculate down capture ratio for each column
    uc_ratio = Vector{Float64}(undef, length(colnames(returns)))
    for (idx, col) in enumerate(colnames(returns))
        avg_portfolio_up = mean(values(portfolio_up[col]))
        avg_benchmark_up = mean(values(benchmark_up))
        if avg_benchmark_up == 0
            uc_ratio[idx] = NaN  # Avoid division by zero
        else
            uc_ratio[idx] = avg_portfolio_up / avg_benchmark_up
        end
    end

    return uc_ratio
end

function overall_capture(returns::TimeArray, benchmark_returns::TimeArray, thresh_value = 0)

    """
    Compute overall capture for each column in a TimeArray of returns.

    Parameters:
    - returns: TimeArray containing portfolio returns
    - benchmark_returns: TimeArray containing benchmark returns
    - thresh_value: Threshold value to determine up markets (default: 0)

    Returns:
    - Vector of overall capture ratios for each column in the TimeArray
    """

    dc = down_capture(returns, benchmark_returns, thresh_value)
    uc = up_capture(returns, benchmark_returns, thresh_value)
    oc_ratio = uc ./ dc

    return oc_ratio

end


function performance_table(ta_returns::TimeArray, benchmark_returns::TimeArray; thresh_value::Number = 0, periods_per_year::Int)

    """
    Compute table with summary performance statistics for asset returns.

    Parameters:
    - ta_returns: TimeArray containing returns for each asset
    - benchmark_returns: TimeArray containing benchmark returns
    - thresh_value: Threshold value for down markets (default: 0)
    - periods_per_year: Number of periods in a year 

    Returns:
    - DataFrame with performance metrics for each asset
    """   

    @assert size(ta_returns, 1) == size(benchmark_returns,1) "Asset returns and benchmark row counts do not match."

    asset_names = colnames(ta_returns)
    annual_returns = annual_return(ta_returns, periods_per_year)
    annual_std = annual_stdev(ta_returns, periods_per_year)
    sharpe_ratio = annual_sharpe_ratio(ta_returns, periods_per_year)
    sortino_ratios = sortino_ratio(ta_returns, thresh_value)
    max_dds = max_drawdown(ta_returns)
    uc_ratio = up_capture(ta_returns, benchmark_returns, thresh_value)
    dc_ratio = down_capture(ta_returns, benchmark_returns, thresh_value)
    oc_ratio = overall_capture(ta_returns, benchmark_returns, thresh_value)

    stats_names = ["Annual Return", "Annual StDev", "Sharpe Ratio", "Sortino Ratio", "Max Drawdowns", 
    "Down Capture", "Up Capture", "Overall Capture"]

    summary_stats_table = DataFrame([annual_returns, annual_std, sharpe_ratio, sortino_ratios, max_dds, 
    dc_ratio, uc_ratio, oc_ratio], :auto)
    summary_stats_table = permutedims(summary_stats_table) .* 100
    summary_stats_table = DataFrames.rename(summary_stats_table, asset_names)


    summary_stats_table[!, "Stat"] = stats_names
    summary_stats_table = select(summary_stats_table, :Stat, asset_names...)

    return summary_stats_table

end




##################################### THE END #################################################
end
