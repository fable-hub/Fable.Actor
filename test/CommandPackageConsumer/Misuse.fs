module CommandPackageMisuse

open Fable.Actor.Commands

// These branches must fail in an external assembly; the normal consumer compiles neither.
#if COMMAND_MESSAGE_MISMATCH
let command: Command<int> = Command.none
let owner = CommandActor.start () (fun () (_: string) -> Next.Continue((), command))
#endif

#if COMMAND_SEND_MISMATCH
let wrongMessage (owner: CommandActor<string>) = CommandActor.send owner 0
#endif
