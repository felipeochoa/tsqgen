import { table } from './select';
import { $, agg, not } from './expression';
import * as sql from './types';
import { unlex } from './serialize';

const posts = table('posts', {
    id: sql.uuid.notNull(),
    name: sql.text.notNull(),
    last_mod_time: sql.timestampWithTimeZone.notNull(),
    last_mod_author: sql.uuid.notNull(),
    word_count: sql.number.notNull(),
    phase: sql.text.notNull(),
    deleted: sql.boolean.notNull(),
});

const userPost = table('user_post', {
    user_id: sql.uuid.notNull(),
    post_id: sql.uuid.notNull(),
    active: sql.boolean.notNull(),
});

const params = $<{userId: sql.Uuid}>({userId: sql.uuid.notNull()});
const query
    = posts.as('s').join(userPost.as('up'), ({s, up}) => s.id.eq(up.post_id))
        .select(({s}) => ({
            id: s.id, name: s.name, last_mod_time: s.last_mod_time,
            last_mod_author: s.last_mod_author,
            word_count: s.word_count,
            phase: s.phase,
        }))
        .where(({s, up}) => not(s.deleted).and(up.user_id.eq(params.userId)));

console.log(unlex(query.serialize()), params({userId: sql.tagUuid('2db78c1c29014a898be5992c675b08eb')}));

const query2 = userPost.as('us')
    .select(() => ({count: agg<number>('COUNT', [])}))
    .where(({us}) => us.user_id.eq(params.userId))
    .scalar();

console.log(unlex(query2.serialize()), params({userId: sql.tagUuid('fc48f576-d8ff-4ed9-a870-32baff3ff985')}));
