module CommandPackageMisuse

open Fable.Actor

#if COMMAND_MESSAGE_MISMATCH
let command: Command<int> = Command.none
let owner = Actor.spawn ((), fun () (_: string) -> (), command)
#endif

#if COMMAND_SEND_MISMATCH
let wrongMessage (owner: Actor<string>) = Actor.send owner 0
#endif

#if ACTOR_REFERENCE_RECEIVE
let stealMessage (reference: Actor<string>) = reference.Receive()
#endif
