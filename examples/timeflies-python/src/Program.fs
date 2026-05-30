module Program

open Fable.Core
open Fable.Python.TkInter
open Fable.Python.AsyncIO
open Fable.Actor

// ---------------------------------------------------------------------------
// TkInter UI setup
// ---------------------------------------------------------------------------

[<Emit("__import__('json').loads($0)")>]
let parseJson (s: string) : obj = nativeOnly

[<Emit("$0[$1]")>]
let getField (o: obj) (key: string) : obj = nativeOnly

let mainAsync =
    async {
        let root = Tk()
        root.title "Fable.Actor Timeflies - Python"

        let frame = Frame(root, width = 800, height = 600, bg = "#1a1a2e")
        frame.pack ()

        // Pre-create labels for each character
        let labels =
            Timeflies.text
            |> Seq.toList
            |> List.mapi (fun _i c ->
                Label(frame, text = string c, fg = "#00ffff", bg = "#1a1a2e"))
            |> Array.ofList

        // sendFn: parse JSON and place the label
        let sendFn (json: string) =
            let data = parseJson json
            let index = unbox<int> (getField data "index")
            let x = unbox<int> (getField data "x")
            let y = unbox<int> (getField data "y")
            labels.[index].place (x, y)

        let distributor = Timeflies.setupPipeline sendFn

        frame.bind (
            "<Motion>",
            fun (ev: Event) ->
                Actor.send distributor { Timeflies.X = ev.x; Timeflies.Y = ev.y }
        )
        |> ignore

        // Async main loop: process tkinter events + yield to asyncio
        while true do
            while root.dooneevent (int Flags.DONT_WAIT) do
                ()

            do! Async.AwaitTask(asyncio.create_task (asyncio.sleep 0.005))
    }

printfn "Started ..."
Async.RunSynchronously mainAsync
