thread apply all bt full
eval "p *((LWLockHandle (*) [%u]) held_lwlocks)", num_held_lwlocks
eval "p *((MyLockedPage (*) [%u]) myLockedPages)", numberOfMyLockedPages
up 99999
set $i=0
set $end=argc
while ($i < $end)
p argv[$i++]
end
quit