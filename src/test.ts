import { table } from './select';
import { $, agg, not } from './expression';
import * as sql from './types';

const posts = table<'posts', {
    id: sql.Uuid;
    name: string;
    last_mod_time: Date;
    last_mod_author: sql.Uuid;
    word_count: number;
    phase: string;
    deleted: boolean;
}>('posts', {
    id: sql.uuid.notNull(),
    name: sql.text.notNull(),
    last_mod_time: sql.timestampWithTimeZone.notNull(),
    last_mod_author: sql.uuid.notNull(),
    word_count: sql.number.notNull(),
    phase: sql.text.notNull(),
    deleted: sql.boolean.notNull(),
});

const userPost = table<'user_post', {
    user_id: sql.Uuid;
    post_id: sql.Uuid;
    active: boolean;
}>('user_post', {
    user_id: sql.uuid.notNull(),
    post_id: sql.uuid.notNull(),
    active: sql.boolean.notNull(),
});

const params = $<{userId: sql.Uuid}>({userId: sql.uuid.notNull()});
const query =
    posts.as('s').join(userPost.as('up'), ({s, up}) => s.id.eq(up.post_id))
        .select(({s}) => ({
            id: s.id, name: s.name, last_mod_time: s.last_mod_time,
            last_mod_author: s.last_mod_author,
            word_count: s.word_count,
            phase: s.phase,
        }))
        .where(({s, up}) => not(s.deleted).and(up.user_id.eq(params.userId)));

const query2 = userPost.as('us')
    .select(() => ({count: agg<number>('COUNT', [])}))
    .where(({us}) => us.user_id.eq(params.userId))
    .scalar();

console.log(query2.serialize());
client.query(query.toString(), params({userId: 'xyz' as sql.Uuid}));
