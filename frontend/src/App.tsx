import { useState } from 'react'
import './App.css'

function App() {
  const [count, setCount] = useState(0)

  return (
      <>
          <h1 className="text-4xl font-bold">Rust + Vite Workspace</h1>
          <h1 className="text-3xl font-bold underline">
              Hello world!!!
          </h1>
          <div className="card">
              <button onClick={() => setCount((count) => count + 1)}>
                  count is {count}
              </button>
              <p>
                  Edit <code>src/App.tsx</code> and save to test HMR
              </p>
          </div>
      </>
  )
}

export default App
