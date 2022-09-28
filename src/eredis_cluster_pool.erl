-module(eredis_cluster_pool).
-behaviour(supervisor).

%% API.
-export([create/6]).
-export([create/7]).
-export([stop/1]).
-export([transaction/2]).

%% Supervisor
-export([start_link/0]).
-export([init/1]).

-include("eredis_cluster.hrl").

create(Host, Port, DataBase, Password, Size, MaxOverflow) ->
    create(Host, Port, DataBase, Password, Size, MaxOverflow, []).
create(Host, Port, DataBase, Password, Size, MaxOverflow, Options) ->
    PoolName = get_name(Host, Port, Options),
    case whereis(PoolName) of
        undefined ->
            WorkerArgs = [{host, Host},
                          {port, Port},
                          {database, DataBase},
                          {password, Password}],
            PoolArgs = [{name, {local, PoolName}},
                        {worker_module, eredis_cluster_pool_worker},
                        {size, Size},
                        {max_overflow, MaxOverflow}],
            ChildSpec = poolboy:child_spec(PoolName, PoolArgs,
                                                case Options of
                                                    [] -> WorkerArgs;
                                                    _ -> WorkerArgs ++ [{options, Options}]
                                                end),
            {Result, _} = supervisor:start_child(?MODULE,ChildSpec),
            {Result, PoolName};
        _ ->
            {ok, PoolName}
    end.

-spec transaction(PoolName::atom(), fun((Worker::pid()) -> redis_result())) ->
    redis_result().
transaction(PoolName, Transaction) ->
    try
        poolboy:transaction(PoolName, Transaction)
    catch
        exit:_ ->
            {error, no_connection}
    end.

-spec stop(PoolName::atom()) -> ok.
stop(PoolName) ->
    supervisor:terminate_child(?MODULE,PoolName),
    supervisor:delete_child(?MODULE,PoolName),
    ok.

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
    -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
    {ok, {{one_for_one, 1, 5}, []}}.

get_name(Host, Port, Options) ->
    Prefix = proplists:get_value(worker_name_prefix, Options, rand_int()),
    list_to_atom(str(Prefix) ++ "_" ++ Host ++ "#" ++ str(Port)).

str(S) when is_integer(S) -> integer_to_list(S);
str(S) when is_binary(S) -> binary_to_list(S);
str(S) when is_atom(S) -> atom_to_list(S);
str(S) when is_list(S) -> S.

rand_int() ->
    <<R:32>> = crypto:strong_rand_bytes(4),
    R.
