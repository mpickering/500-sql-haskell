{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}
--{-# OPTIONS_GHC -ddump-splices #-}
module Test where

import qualified Compiler as C
import qualified LMS as L
import qualified Interpreter as I


main = do
  putStrLn "Interpreter"
  I.runQuery I.query
  I.runQuery I.queryJoin

  putStrLn "Compiler"
  $$(C.runQuery C.query)
  $$(C.runQuery C.query2)
  $$(C.runQuery C.queryJoin)

  putStrLn "LMS Compiler"
  $$(L.runQuery L.query)
  $$(L.runQuery L.query2)
  $$(L.runQuery L.queryJoin)

  putStrLn "LMS Interpreter"
  L.runQueryUnstaged L.query
  L.runQueryUnstaged L.query2
  L.runQueryUnstaged L.queryJoin

