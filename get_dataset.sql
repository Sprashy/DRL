SELECT DISTINCT
    trs.id,
    trs.match_id,
    trs.match_period,
    trs.minute,
    trs.second,
    trs.vide_timestamp,
    trs.type::text,
    trs.location::text,
    trs.team_id,
    trs.opponent_team_id,
    trs.player_id,
    trs.shot::text,
    trs.pass::text,
    trs.ground_duel::text,
    trs.aerial_duel::text,
    trs.infraction::text,
    starter.side
FROM
    soccer_p_match_event trs
JOIN
    soccer_t_match_starter starter ON trs.match_id = starter.match_id AND trs.team_id = starter.team_id
WHERE
    trs.competition_id = %(competition_id)s AND trs.match_id = ANY(%(match_id)s)