module TrackerCommons

import JSON

type CommonWorm
    id
    t::Array{Float64,1}
    x::Array{Array{Float64,1},1}
    y::Array{Array{Float64,1},1}
end

type WormDataSet
    data::Array{CommonWorm,1}
    units::Dict{String,Any}
end

function make_dbl(a)
    x::Float64 = isa(a, Number) ? convert(Float64, a) : nan(Float64)
    x
end

function make_dbl_array(q::Any)
    if (typeof(q) <: Array) map(q) do qi; make_dbl(qi); end
    else [make_dbl(q)]
    end
end

function make_dbl_array(q::Any, n::Int64)
    if (typeof(q) <: Array) map(q) do qi; make_dbl(qi); end
    else fill(make_dbl(q), n)
    end
end

function make_dbl_array_array(q::Any, n::Int64)
    if (n == 1)
        if (length(q)==1 && typeof(q[1]) <: Array)
            Array[make_dbl_array(q[1])]
        else
            Array[make_dbl_array(q)]
        end
    else
        result = fill(Array{Float64, 1}[], n)
        for i in 1:length(q)
            result[i] = make_dbl_array(q[i])
        end
        result
    end
end

function get_a_worm(data::Dict{String, Any})
    id = data["id"]
    t = make_dbl_array(data["t"])
    ox = haskey(data, "ox") ? make_dbl_array(data["ox"], length(t)) : (haskey(data, "cx") ? make_dbl_array(data["cx"], length(t)) : zeros(length(t)))
    oy = haskey(data, "oy") ? make_dbl_array(data["oy"], length(t)) : (haskey(data, "cy") ? make_dbl_array(data["cy"], length(t)) : zeros(length(t)))
    x = make_dbl_array_array(data["x"], length(t))
    y = make_dbl_array_array(data["y"], length(t))
    if (length(t) != length(ox))
        error("Length of t and x-offset or centroid do not match for worm $(id) at $(t[1])")
    end
    if (length(t) != length(oy))
        error("Length of t and y-offset or centroid do not match for worm $(id) at $(t[1])")
    end
    if (length(t) != length(x))
        error("Length of t and x data do not match for worm $(id) at $(t[1]): $(length(t)) vs $(length(x))")
    end
    if (length(t) != length(y))
        error("Length of t and y data do not match for worm $(id) at $(t[1]): $(length(t)) vs $(length(y))")
    end
    for i in 1:length(t)
        if (length(x[i]) != length(y[i]))
            error("Different number of x and y values for data point $(i) at time $(t[i]) for worm $(id)")
        end
        x[i] = x[i] - ox[i]
        y[i] = y[i] - oy[i]
    end
    CommonWorm(id, t, x, y)
end

function read_wcon(filename::String)
    j = JSON.parsefile(filename)
    j = convert(Dict{String, Any}, j)
    if (!haskey(j, "tracker-commons"))
        println("Warning, file does not identify as tracker-commons")
    elseif (j["tracker-commons"] != true)
        println("Warning, file does not specify that tracker-commons is false")
    end
    u = convert(Dict{String, Any}, j["units"])
    for x in ["t" "x" "y"]
        if (!haskey(u,x))
            error("WCON file does not specify units for $(t)")
        end
    end
    d = j["data"]
    if (isa(d, Dict{String, Any}))
        d = [d]
    end
    if (!(typeof(d) <: Array))
        error("WCON data section is not an array")
    end
    dd = [convert(Dict{String, Any}, x) for x in d]
    WormDataSet([get_a_worm(x) for x in dd], u)
end

function write_wcon(worms::WormDataSet, filename::String)
    h = open(filename, "w")
    JSON.print(h, {"tracker-commons" => true, "units" => worms.units, "data" => worms.data})
    close(h)
end

end