%% @doc The difficuly a hash pointing to a candidate SPoA has to satisfy.
-define(SPORA_SLOW_HASH_DIFF(Height), fun() ->
	Forks = {
		ar_fork:height_2_3()
	},
	case Forks of
		{Fork_2_3} when Height >= Fork_2_3 ->
			0
	end
end()).

%% @doc The size of the search space - a share of the weave randomly sampled
%% at every block. The solution must belong to the search space.
-define(SEARCH_SPACE_SIZE(Height), fun() ->
	Forks = {
		ar_fork:height_2_3()
	},
	case Forks of
		{Fork_2_3} when Height >= Fork_2_3 ->
			1024 * 1024 * 1024 * 1024 % 1 TB
	end
end()).

%% @doc The number of contiguous subspaces of the search space, a roughly equal
%% share of the search space is sampled from each of the subspaces.
-define(SPORA_SEARCH_SPACE_SUBSPACES_COUNT(Height), fun() ->
	Forks = {
		ar_fork:height_2_3()
	},
	case Forks of
		{Fork_2_3} when Height >= Fork_2_3 ->
			1024
	end
end()).

%% @doc The minimum difficulty allowed.
-define(SPORA_MIN_DIFFICULTY(Height), fun() ->
	Forks = {
		ar_fork:height_2_3()
	},
	case Forks of
		{Fork_2_3} when Height >= Fork_2_3 ->
			1 % TODO
	end
end()).
