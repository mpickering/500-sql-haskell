{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
module Test where

--import qualified Compiler as C
import qualified LMS as L
--import qualified Interpreter as I
--import qualified SimpleInterpreter as SI
import qualified StreamLMS as S
import Weigh
import GHC.Stats
import System.Mem

--csvQuery = L.Filter (L.Eq (L.Value "cricket") (L.Field "word")) csvTable

--csvTable = L.Scan ["word", "year", "n1" "n2"] "1gram.csv"

main :: IO ()
main = do
  {-
  putStrLn "Simple Interpreter"
  SI.runQuery SI.query
  SI.runQuery SI.queryJoin


  putStrLn "Interpreter"
  I.runQuery I.query
  I.runQuery I.queryJoin

  putStrLn "Compiler"
  $$(C.runQuery C.queryProj)
  $$(C.runQueryL C.queryProj)
  $$(C.runQuery C.query)
  $$(C.runQuery C.query2)
  $$(C.runQuery C.queryJoin)
-}
  putStrLn "LMS Compiler"
  {-
  mainWith ( do
      action "q1" $$(S.runQuery S.query)
      action "q2" $$(S.runQuery S.query2)
      action "q3" $$(S.runQuery S.queryJoin)
      action "q4" $$(S.runQuery S.queryP)
      )
      -}

  $$(S.runQuery S.query)
  $$(S.runQuery S.query2)
  $$(S.runQuery S.queryJoin)
  $$(S.runQuery S.queryP)
  performGC
  s1 <- getRTSStats

  $$(L.runQuery L.query)
  $$(L.runQuery L.query2)
  $$(L.runQuery L.queryJoin)
  $$(L.runQuery L.queryP)
  performGC
  s2 <- getRTSStats

  -- Observe that the number of max_live_bytes is much lower for the
  -- streaming version. However, the number of allocated bytes is the same
  -- for each example as we must read every character eventually.
  print (max_live_bytes s1)
  print (max_live_bytes s2)




--
  putStrLn "LMS Compiler Stream"

  putStrLn "LMS Interpreter"
--  L.runQueryUnstaged L.query
--  L.runQueryUnstaged L.query2
--  L.runQueryUnstaged L.queryJoin
