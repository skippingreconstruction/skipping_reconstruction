#include "metadata/complex_boundary.h"
#include <queue>

ComplexBoundary::ComplexBoundary(const Boundary &b)
{
    const auto &i = b.getIntervals();
    for (auto it = i.begin(); it != i.end(); it++)
        this->intervals[it->first] = {it->second};
}

shared_ptr<ComplexBoundary> ComplexBoundary::makeComplexBoundary(
    const vector<shared_ptr<const Boundary>> &boundaries,
    int max_intervals_per_attribute)
{
    assert(max_intervals_per_attribute >= 1);
    shared_ptr<ComplexBoundary> ans =
        shared_ptr<ComplexBoundary>(new ComplexBoundary());

    for (auto b : boundaries)
        for (auto it : b->getIntervals())
            ans->intervals[it.first].push_back(it.second);

    // remove an attribute if any boundary miss that attribute. In such
    // case, the unioned interval is unbounded
    for (auto it = ans->intervals.begin(); it != ans->intervals.end();)
        if (it->second.size() != boundaries.size())
            it = ans->intervals.erase(it);
        else
            it++;

    // union non-disjoint or neighbor intervals
    auto is_unionable = [](shared_ptr<const Interval> i,
                           shared_ptr<const Interval> j) -> bool {
        SET_RELATION rel = i->relationship(*j);
        if (rel != SET_RELATION::DISJOINT)
            return true;
        auto i_prev = i->getMin()->clone();
        auto j_prev = j->getMin()->clone();
        if (!i_prev->prev() || !j_prev->prev())
            return false;

        if (i_prev->cmp(j->getMax().get()) == 0 ||
            j_prev->cmp(i->getMax().get()) == 0)
            return true;

        return false;
    };
    for (auto it = ans->intervals.begin(); it != ans->intervals.end();
         it++)
    {
        unordered_set<shared_ptr<const Interval>> s(it->second.begin(),
                                                    it->second.end());
        it->second.clear();
        while (s.size())
        {
            auto i = *s.begin();
            s.erase(i);
            vector<shared_ptr<const Interval>> v = {i};
            for (auto it = s.begin(); it != s.end();)
            {
                if (is_unionable(i, *it))
                {
                    v.push_back(*it);
                    it = s.erase(it);
                }
                else
                    it++;
            }

            if (v.size() == 1)
                it->second.push_back(v[0]);
            else
                s.insert(make_shared<Interval>(Interval::Union(v)));
        }
    }

    // union intervals if an attribute has many intervals
    auto extra_distance = [](shared_ptr<const Interval> i,
                             shared_ptr<const Interval> j) -> double {
        assert(i->relationship(*j) == SET_RELATION::DISJOINT);
        const auto &m = Interval::Union({i, j});
        double dm = m.getMax()->distance(m.getMin().get());
        double di = i->getMax()->distance(i->getMin().get());
        double dj = j->getMax()->distance(j->getMin().get());
        assert(dm >= di + dj);
        return dm - di - dj;
    };
    for (auto it = ans->intervals.begin(); it != ans->intervals.end();
         it++)
    {
        auto inters = it->second;
        int extra_num = inters.size() - max_intervals_per_attribute;
        if (extra_num <= 0)
            continue;
        priority_queue<tuple<double, int, int>,
                       vector<tuple<double, int, int>>,
                       std::greater<tuple<double, int, int>>>
            que;
        for (int i = 0; i < inters.size(); i++)
            for (int j = i + 1; j < inters.size(); j++)
                que.push(make_tuple(
                    extra_distance(inters[i], inters[j]), i, j));

        while (extra_num > 0)
        {
            auto p = que.top();
            que.pop();
            int i = std::get<1>(p), j = std::get<2>(p);
            if (!inters[i] || !inters[j])
                continue;
            auto m = make_shared<Interval>(
                Interval::Union({inters[i], inters[j]}));
            inters[i] = inters[j] = nullptr;
            inters.push_back(m);
            extra_num--;
            for (int k = 0; k < inters.size() - 1; k++)
                if (inters[k] != nullptr)
                    que.push(make_tuple(extra_distance(inters[k], m), k,
                                        inters.size() - 1));
        }
        it->second.clear();
        std::copy_if(
            inters.begin(), inters.end(),
            std::back_inserter(it->second),
            [](shared_ptr<const Interval> i) { return i != nullptr; });
        assert(it->second.size() == max_intervals_per_attribute);
    }
    return ans;
}

void ComplexBoundary::keepAttributes(
    const unordered_set<string> &attributes)
{
    for (auto it = intervals.begin(); it != intervals.end();)
        if (attributes.count(it->first) == 0)
            it = intervals.erase(it);
        else
            it++;
}

SET_RELATION ComplexBoundary::relationship(const Boundary &other) const
{
    // fill the missing attributes of the two boundaries
    auto b = this->intervals;
    auto b_other = other.getIntervals();
    for (auto it = b.begin(); it != b.end(); it++)
    {
        auto it_o = b_other.find(it->first);
        if (it_o == b_other.end())
            b_other[it->first] = make_shared<const Interval>(
                getMinValue(it->first), false, getMaxValue(it->first),
                false);
        else if (it->second[0]->getType() != it_o->second->getType())
            throw Exception(
                "ComplexBoundary: Boundaries do not have the same "
                "type at attribute " +
                it->first);
    }
    for (auto it_o = b_other.begin(); it_o != b_other.end(); it_o++)
        if (b.find(it_o->first) == b.end())
            b[it_o->first].push_back(make_shared<const Interval>(
                getMinValue(it_o->first), false,
                getMaxValue(it_o->first), false));

    SET_RELATION relation = SET_RELATION::EQUAL;
    for (auto it = b.begin(); it != b.end(); it++)
    {
        auto it_o = b_other.find(it->first);
        auto r = relationship(it->second, it_o->second);
        switch (r)
        {
        case SET_RELATION::DISJOINT:
            return SET_RELATION::DISJOINT;
            break;
        case SET_RELATION::EQUAL:
            break;
        case SET_RELATION::INTERSECT:
            relation = SET_RELATION::INTERSECT;
            break;
        case SET_RELATION::SUBSET:
            if (relation == SET_RELATION::SUBSET ||
                relation == SET_RELATION::EQUAL)
                relation = SET_RELATION::SUBSET;
            else
                relation = SET_RELATION::INTERSECT;
            break;
        case SET_RELATION::SUPERSET:
            if (relation == SET_RELATION::SUPERSET ||
                relation == SET_RELATION::EQUAL)
                relation = SET_RELATION::SUPERSET;
            else
                relation = SET_RELATION::INTERSECT;
            break;
        default:
            throw Exception("ComplexBoundary::relationship: Unknow "
                            "interval relation");
            break;
        }
    }
    return relation;
}

SET_RELATION ComplexBoundary::relationship(
    const vector<shared_ptr<const Interval>> &complex,
    shared_ptr<const Interval> plain) const
{
    assert(complex.size() > 0);
    vector<SET_RELATION> relations;
    for (auto i : complex)
        relations.push_back(i->relationship(*plain));

    // disjoint if all intervals in complex are disjoint with the plain
    bool all_disjoint = true;
    for (int i = 0; i < relations.size() && all_disjoint; i++)
        if (relations[i] != SET_RELATION::DISJOINT)
            all_disjoint = false;
    if (all_disjoint)
        return SET_RELATION::DISJOINT;

    // equal if the complex has one interval and the interval is equal
    // to the plain
    if (relations.size() == 1 && relations[0] == SET_RELATION::EQUAL)
        return SET_RELATION::EQUAL;

    // subset if all intervals in complex are the subset of the plain
    bool all_subset = true;
    for (int i = 0; i < relations.size() && all_subset; i++)
        if (relations[i] != SET_RELATION::SUBSET)
            all_subset = false;
    if (all_subset)
        return SET_RELATION::SUBSET;

    // superset if 1) an interval in the complex is the superset of
    // plain or 2) an interval in complex is equal to the plain and
    // complex at least has one more interval
    for (int i = 0; i < relations.size(); i++)
    {
        if (relations[i] == SET_RELATION::SUPERSET)
            return SET_RELATION::SUPERSET;
        else if (relations[i] == SET_RELATION::EQUAL &&
                 relations.size() > 1)
            return SET_RELATION::SUPERSET;
    }
    return SET_RELATION::INTERSECT;
}

shared_ptr<FunctionExpression> ComplexBoundary::makeExpression() const
{
    if (intervals.size() == 0)
        return nullptr;

    auto make_expression =
        [](string attr,
           const vector<shared_ptr<const Interval>> &intervals)
        -> shared_ptr<FunctionExpression> {
        assert(intervals.size() > 0);
        if (intervals.size() == 1)
            return intervals[0]->makeExpression(attr);

        vector<shared_ptr<const Expression>> exps;
        for (auto i : intervals)
            exps.push_back(i->makeExpression(attr));
        return FunctionExpression::connectExpression("filter_exp", exps,
                                                     false, false);
    };

    auto it = intervals.begin();
    if (intervals.size() == 1)
        return make_expression(it->first, it->second);

    vector<shared_ptr<const Expression>> exps;
    for (; it != intervals.end(); it++)
        exps.push_back(make_expression(it->first, it->second));
    return FunctionExpression::connectExpression("filter_exp", exps,
                                                 false, true);
}

ComplexBoundary ComplexBoundary::intersect(const Boundary &other) const
{
    if (this->relationship(other) == SET_RELATION::DISJOINT)
        throw Exception(
            "ComplexBoundary::intersect: two bondaries are disjoint");

    auto other_intervals = other.getIntervals();
    unordered_map<string, vector<shared_ptr<const Interval>>> m;
    for (auto it = this->intervals.begin(); it != intervals.end(); it++)
    {
        auto it1 = other_intervals.find(it->first);
        if (it1 == other_intervals.end())
            m[it->first] = it->second;
        else
        {
            for (auto i : it->second)
                if (i->relationship(*it1->second) !=
                    SET_RELATION::DISJOINT)
                {
                    m[it->first].push_back(make_shared<const Interval>(
                        i->interesct(*it1->second)));
                }
        }
        assert(m[it->first].size() > 0);
    }
    for (auto it1 = other_intervals.begin();
         it1 != other_intervals.end(); it1++)
        if (m.find(it1->first) == m.end())
            m[it1->first] = {it1->second};
    return ComplexBoundary(m);
}