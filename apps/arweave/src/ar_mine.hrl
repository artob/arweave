%% @doc The difficuly a hash pointing to a candidate SPoA has to satisfy.
-define(SPORA_SLOW_HASH_DIFF, 0).

%% @doc The number of contiguous subspaces of the search space, each roughly of equal size.
-define(SPORA_SEARCH_SPACE_SUBSPACES_COUNT, 100).

%% @doc How many intervals are chosen on every search subspace.
%% Essentially, it means a miner must have at least one SPoA
%% from the chosen 1 / SPORA_SEARCH_SUBSPACE_INTERVAL_COUNT of the weave
%% in order to mine.
-define(SPORA_SEARCH_SUBSPACE_INTERVAL_COUNT, 4).

%% @doc The minimum difficulty allowed.
-define(SPORA_MIN_DIFFICULTY, 1). % TODO
