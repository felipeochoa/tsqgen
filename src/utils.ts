export function assertNever(_: never, msg: string): never { // Useful for Typescript exhaustiveness checks
    throw new Error(msg);
}
