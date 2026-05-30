module App

open Browser.Dom
open Browser.Types
open Fable.Core
open Feliz
open Fable.Actor

// ---------------------------------------------------------------------------
// React app
// ---------------------------------------------------------------------------

[<Emit("JSON.parse($0)")>]
let parseJson (s: string) : obj = nativeOnly

[<Emit("$0[$1]")>]
let getField (o: obj) (key: string) : obj = nativeOnly

[<ReactComponent>]
let App () =
    // Mutable positions (actors update this directly)
    let posRef = React.useRef (Array.init Timeflies.text.Length (fun _ -> (-100, -100)))
    // Counter to trigger re-renders
    let _, setTick = React.useState 0
    let tickRef = React.useRef 0

    // Ref to hold the distributor actor
    let distributorRef = React.useRef<Actor<Timeflies.MousePos> option> None

    // Spawn actor pipeline on mount
    React.useEffectOnce (fun () ->
        let sendFn (json: string) =
            let data = parseJson json
            let index = unbox<int> (getField data "index")
            let x = unbox<int> (getField data "x")
            let y = unbox<int> (getField data "y")
            posRef.current.[index] <- (x, y)
            tickRef.current <- tickRef.current + 1
            setTick tickRef.current

        let distributor = Timeflies.setupPipeline sendFn
        distributorRef.current <- Some distributor

        { new System.IDisposable with
            member _.Dispose() = Actor.kill distributor })

    // Listen for mouse moves globally
    React.useEffectOnce (fun () ->
        let handler (ev: Event) =
            let me = ev :?> MouseEvent

            distributorRef.current
            |> Option.iter (fun d ->
                Actor.send d { Timeflies.X = int me.clientX; Timeflies.Y = int me.clientY })

        document.addEventListener ("mousemove", handler)

        { new System.IDisposable with
            member _.Dispose() =
                document.removeEventListener ("mousemove", handler) })

    Html.div [
        prop.style [
            style.width (length.vw 100)
            style.height (length.vh 100)
        ]
        prop.children [
            for i in 0 .. Timeflies.text.Length - 1 do
                let x, y = posRef.current.[i]

                Html.span [
                    prop.key i
                    prop.text (string Timeflies.text.[i])
                    prop.style [
                        style.position.fixedRelativeToWindow
                        style.left (length.px x)
                        style.top (length.px y)
                        style.color "#00ffff"
                        style.fontSize 18
                        style.fontFamily "monospace"
                        style.fontWeight.bold
                        style.pointerEvents.none
                    ]
                ]
        ]
    ]

// ---------------------------------------------------------------------------
// Mount
// ---------------------------------------------------------------------------

let root =
    ReactDOM.createRoot (document.getElementById "app")

root.render (App())
